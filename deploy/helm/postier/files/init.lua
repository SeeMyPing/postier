-- SPDX-FileCopyrightText: 2026 Nawer Kezal
-- SPDX-License-Identifier: AGPL-3.0-only

-- Postier's KumoMTA policy.
--
-- This file is fixed: it ships with Postier and holds no business logic and
-- no per-installation value. Everything that varies (sending domains, DKIM
-- selectors, routes, queue timings) lives in a JSON data file that Postier
-- generates, re-read at most every DATA_TTL. Adding a sending domain is
-- therefore a file change, never a restart of the MTA.
--
-- Secrets come from the environment, not from the data file:
--   POSTIER_KUMO_INJECT_PASSWORD  basic-auth password of the injection API
--   POSTIER_INGEST_TOKEN          bearer token sent with the log records
--
-- Tested against the KumoMTA version pinned in deploy/ (see README).

local kumo = require 'kumo'

local DATA_PATH = os.getenv 'POSTIER_KUMO_DATA'
  or '/opt/kumomta/etc/postier/data.json'
local DATA_TTL = os.getenv 'POSTIER_KUMO_DATA_TTL' or '60 seconds'
local INJECT_USER = os.getenv 'POSTIER_KUMO_INJECT_USER' or 'postier'
local INJECT_PASSWORD = os.getenv 'POSTIER_KUMO_INJECT_PASSWORD' or ''
local INGEST_TOKEN = os.getenv 'POSTIER_INGEST_TOKEN' or ''

-- The log hook and the pseudo-domain of its queue. The ".log_hook" suffix can
-- never collide with a real TLD.
local HOOK_NAME = 'postier'
local HOOK_QUEUE = 'postier.log_hook'
local HOOK_CONSTRUCTOR = 'make.' .. HOOK_QUEUE

-- Message metadata copied into every log record. Application keys carry the
-- x_ prefix KumoMTA reserves for users; `tenant` is KumoMTA's own key and
-- also names the queue (one queue per tenant and destination domain).
local META_ID = 'x_postier_id'
local META_TRACE = 'x_postier_trace'
local LOGGED_META = { META_ID, META_TRACE, 'tenant' }

-- Must be created at file scope, not inside an event handler.
local load_data = kumo.memoize(function()
  return kumo.json_load(DATA_PATH)
end, {
  name = 'postier_data',
  ttl = DATA_TTL,
  capacity = 1,
})

local function section(name)
  return load_data()[name] or {}
end

-------------------------------------------------------------------------------
-- Startup
-------------------------------------------------------------------------------

kumo.on('init', function()
  local listen = section 'listen'
  local hook = section 'log_hook'

  kumo.define_spool {
    name = 'data',
    path = '/var/spool/kumomta/data',
    kind = 'RocksDB',
  }
  kumo.define_spool {
    name = 'meta',
    path = '/var/spool/kumomta/meta',
    kind = 'RocksDB',
  }

  -- Local logs are a safety net for the operator. Postgres is the record;
  -- these files are what `postier recover` will compare it against.
  kumo.configure_local_logs {
    log_dir = '/var/log/kumomta',
    max_segment_duration = '1 minute',
    meta = LOGGED_META,
  }

  -- Every log record becomes a message in KumoMTA's own durable queue, then
  -- is POSTed to `postier ingest`: records survive an ingest outage.
  kumo.configure_log_hook {
    name = HOOK_NAME,
    meta = LOGGED_META,
    -- Message-ID is how an ARF complaint is tied back to a message.
    headers = { 'Message-ID' },
  }

  kumo.configure_bounce_classifier {
    files = { '/opt/kumomta/share/bounce_classifier/iana.toml' },
  }

  -- Injection and admin API. Never exposed outside the system namespace:
  -- every caller authenticates, except the pod itself.
  kumo.start_http_listener {
    listen = listen.http or '0.0.0.0:8000',
    trusted_hosts = { '127.0.0.1', '::1' },
  }

  -- Inbound SMTP, for asynchronous bounces and feedback loops only. Nobody
  -- relays through it: smtp_server_rcpt_to below refuses every recipient
  -- outside the return-path and feedback domains, whatever the client.
  kumo.start_esmtp_listener {
    listen = listen.smtp or '0.0.0.0:2525',
    hostname = load_data().hostname,
  }

  if hook.batch_size and hook.batch_size < 1 then
    error 'log_hook.batch_size must be at least 1'
  end
end)

-------------------------------------------------------------------------------
-- Injection (postier dispatch -> KumoMTA)
-------------------------------------------------------------------------------

kumo.on('http_server_validate_auth_basic', function(user, password)
  -- An empty password in the environment disables the API rather than
  -- opening it.
  if INJECT_PASSWORD == '' or password == nil or password == '' then
    return false
  end
  return user == INJECT_USER and password == INJECT_PASSWORD
end)

kumo.on('http_message_generated', function(msg)
  -- dispatch sends the identifiers as per-recipient metadata, which KumoMTA
  -- stores under `extra`. Promote them to top-level keys so that they are
  -- logged with every record of this message.
  local extra = msg:get_meta 'extra'
  if type(extra) ~= 'table' or not extra.postier_id then
    kumo.reject(550, '5.7.1 injection without a postier_id')
  end
  msg:set_meta(META_ID, extra.postier_id)
  msg:set_meta(META_TRACE, extra.trace or '')
  msg:set_meta('tenant', extra.tenant or 'default')

  local from_domain = msg:from_header().domain
  local dom = section('domains')[from_domain]
  if not dom then
    -- Postier checks the domain before accepting; reaching this line means
    -- the data file lags behind. dispatch retries.
    kumo.reject(451, '4.7.1 sending domain not configured yet: ' .. from_domain)
  end

  msg:dkim_sign(kumo.dkim.rsa_sha256_signer {
    domain = from_domain,
    selector = dom.selector,
    headers = {
      'From',
      'To',
      'Subject',
      'Date',
      'Message-ID',
      'Reply-To',
      'MIME-Version',
      'Content-Type',
    },
    key = dom.key_file,
  })
end)

-------------------------------------------------------------------------------
-- Inbound (bounces and feedback loops -> KumoMTA)
-------------------------------------------------------------------------------

-- relay_hosts defaults to the local host and cannot be set to an empty list
-- from Lua, so the rule is enforced here instead: this listener only takes
-- mail addressed to Postier itself.
kumo.on('smtp_server_rcpt_to', function(rcpt, conn_meta)
  if not section('inbound_domains')[string.lower(rcpt.domain)] then
    kumo.reject(550, '5.7.1 relaying denied')
  end
end)

kumo.on('get_listener_domain', function(domain, listener, conn_meta)
  if section('inbound_domains')[domain] then
    -- Parse the report, log an OOB or Feedback record, drop the message.
    -- Anything that is not a report is refused: relay_to stays false.
    return kumo.make_listener_domain {
      log_oob = 'LogThenDrop',
      log_arf = 'LogThenDrop',
    }
  end
end)

-------------------------------------------------------------------------------
-- Queues and delivery
-------------------------------------------------------------------------------

-- A route overrides MX resolution for a destination domain ("*" for all).
-- Production has none; the test bench and the quickstart send everything to
-- a local sink.
local function route_for(domain)
  local routes = section 'routes'
  return routes[domain] or routes['*']
end

kumo.on('get_queue_config', function(domain, tenant, campaign, routing_domain)
  if domain == HOOK_QUEUE then
    local hook = section 'log_hook'
    return kumo.make_queue_config {
      -- Records are small and ingest is idempotent: retry fast, keep long.
      retry_interval = hook.retry_interval or '5 seconds',
      max_retry_interval = hook.max_retry_interval or '1 minute',
      max_age = hook.max_age or '7 days',
      protocol = {
        custom_lua = {
          constructor = HOOK_CONSTRUCTOR,
          batch_size = hook.batch_size or 100,
          -- Unset by default: KumoMTA then sends whatever is ready, without
          -- waiting, and batches only form under backlog. Setting both
          -- trades a little latency for fewer, larger transactions.
          min_batch_size = hook.min_batch_size,
          max_batch_latency = hook.max_batch_latency,
        },
      },
    }
  end

  local queue = section 'queue'
  local config = {
    retry_interval = queue.retry_interval or '20 minutes',
    max_retry_interval = queue.max_retry_interval or '4 hours',
    max_age = queue.max_age or '3 days',
  }
  local route = route_for(domain)
  if route then
    config.protocol = { smtp = { mx_list = { route.host } } }
  end
  return kumo.make_queue_config(config)
end)

kumo.on('get_egress_path_config', function(domain, egress_source, site_name)
  if domain == HOOK_QUEUE then
    local hook = section 'log_hook'
    return kumo.make_egress_path {
      connection_limit = hook.connections or 4,
      -- Do not let a slow ingest hold batches back: fail and retry.
      max_ready = 100000,
    }
  end

  local route = route_for(domain)
  if route then
    return kumo.make_egress_path {
      smtp_port = route.port or 25,
      enable_tls = 'Disabled',
      connection_limit = route.connections or 10,
    }
  end

  return kumo.make_egress_path {
    enable_tls = 'OpportunisticInsecure',
    enable_mta_sts = true,
    enable_dane = true,
  }
end)

-------------------------------------------------------------------------------
-- Log hook (KumoMTA -> postier ingest)
-------------------------------------------------------------------------------

kumo.on('should_enqueue_log_record', function(msg, hook_name)
  if hook_name ~= HOOK_NAME then
    return
  end
  local record = msg:get_meta 'log_record'
  -- Do not log that we logged that we logged...
  if record.reception_protocol == 'LogRecord' then
    return false
  end
  msg:set_meta('queue', HOOK_QUEUE)
  return true
end)

kumo.on(HOOK_CONSTRUCTOR, function(domain, tenant, campaign)
  local hook = section 'log_hook'
  if not hook.url then
    error 'log_hook.url is missing from the Postier data file'
  end

  local connection = {}
  local client = kumo.http.build_client {}

  -- ingest answers 2xx only once the whole batch is committed. Anything
  -- else is a transient failure for every record of the batch: KumoMTA keeps
  -- them and retries. A 5xx disposition would drop them, so never use one.
  function connection:send_batch(messages)
    local records = {}
    for _, m in ipairs(messages) do
      table.insert(records, m:get_meta 'log_record')
    end

    local response = client
      :post(hook.url)
      :header('Content-Type', 'application/json')
      :header('Authorization', 'Bearer ' .. INGEST_TOKEN)
      :body(kumo.serde.json_encode(records))
      :send()

    local disposition = string.format(
      '%d %s',
      response:status_code(),
      response:status_reason() or ''
    )
    if response:status_is_success() then
      return disposition
    end
    kumo.reject(400, disposition .. ': ' .. response:text())
  end

  -- Used instead of send_batch when log_hook.batch_size is 1.
  function connection:send(message)
    return connection:send_batch { message }
  end

  function connection:close()
    client:close()
  end

  return connection
end)

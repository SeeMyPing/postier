# Plan d'implémentation de Postier (côté logiciel)

Ce plan part du [document d'architecture](architecture.md) (13 novembre 2026) et le complète. Il argumente d'abord le choix du langage, puis liste les trous et contradictions du document à régler avant de coder, décrit l'organisation du code, découpe le travail en phases et termine par les décisions encore ouvertes.

---

## 1. Choix du langage : Go

### Ce que le langage doit faire ici

Postier ne fait pas le travail lourd en CPU : KumoMTA, en Rust, gère la file, le DKIM et la livraison. Le code de Postier est surtout de l'orchestration d'entrées/sorties : HTTP, SMTP de soumission, Postgres, appels à l'API Kubernetes. Les critères qui comptent sont donc :

1. la qualité de l'écosystème pour écrire un opérateur Kubernetes ;
2. de bonnes bibliothèques Postgres et une file de jobs adossée à Postgres ;
3. un serveur SMTP de soumission solide ;
4. un binaire statique, multi-architecture, dans une image distroless ;
5. **la productivité d'une seule personne** et la facilité à trouver des contributeurs.

### Comparaison

| Critère | **Go** | Rust | Kotlin/Java | TypeScript | Elixir |
|---|---|---|---|---|---|
| Opérateur K8s | **controller-runtime / kubebuilder : la référence** (cert-manager, CNPG, external-dns) | kube-rs, correct, mais webhooks et génération de CRD moins outillés | JOSDK, fabric8 : utilisables, peu répandus | faible | quasi inexistant |
| File de jobs sur Postgres | **River** (MPL-2.0), insertion transactionnelle avec pgx | à construire | db-scheduler, JobRunr | graphile-worker | Oban (excellent) |
| Accès SQL typé | **pgx + sqlc** | sqlx (très bon) | jOOQ | Kysely | Ecto |
| Serveur SMTP | **emersion/go-smtp**, en production | samotop, peu maintenu | SubEthaSMTP, vieillissant | smtp-server (Nodemailer) | gen_smtp |
| Binaire, image, démarrage | statique, ~30 Mo, démarre en ms | statique, plus léger | JVM lourde, ou GraalVM complexe | Node à embarquer | release BEAM |
| Compilation croisée amd64/arm64 | `GOARCH=arm64`, trivial | correct, plus lent | — | — | — |
| Courbe d'apprentissage, contributeurs | faible, et c'est la langue de l'écosystème K8s | forte (async, lifetimes) | moyenne | faible | niche |

### Pourquoi Go

- **L'opérateur est la différenciation du projet** (voir le tableau des risques : « différenciation sur les CRD, l'opérateur et le GitOps »). En Go, kubebuilder génère les types, les manifestes de CRD, les validations CEL par marqueurs, les webhooks d'admission et de conversion, et envtest permet de tester sur un vrai apiserver. C'est le chemin le plus court et le mieux documenté.
- **Un seul langage pour tout le serveur.** Elixir aurait été excellent pour `ingest` et `webhooks`, avec Oban, mais il faudrait écrire l'opérateur dans un second langage. Pour une personne seule, c'est rédhibitoire.
- **River joue le rôle d'outbox transactionnelle** : on insère le job dans la même transaction que le message (`InsertTx`). Le principe « accepté = persisté » tombe donc gratuitement, sans broker.
- **Rust n'apporte rien de décisif ici.** Son atout (performance, absence de GC) sert sur le chemin chaud de la livraison, et ce chemin est déjà en Rust dans KumoMTA. L'intégration avec KumoMTA passe par HTTP et par Lua, pas par une bibliothèque partagée : écrire Postier en Rust ne permettrait aucun partage de code. En échange, on paierait des compilations lentes, l'async complexe et un écosystème d'opérateurs moins mûr. Les pauses du GC de Go (sous la milliseconde) sont négligeables quand l'objectif de latence se compte en dizaines de ms, dominées par le commit synchrone Postgres.
- **Go colle au modèle « un binaire, plusieurs rôles »**, comme Postal : un seul `main` avec des sous-commandes et une seule image signée.

### Faiblesses de Go et parades

| Faiblesse | Parade |
|---|---|
| **MJML est une bibliothèque Node** : impossible à compiler en Go pur | **mrml** (portage Rust de MJML) compilé en WASM et exécuté avec **wazero** (WASM en Go pur, sans cgo). L'image reste distroless et statique. |
| Gestion d'erreurs verbeuse | `errors.Is/As`, erreurs typées par domaine, traduites en codes RFC 9457 en un seul point |
| Pas de constructeur MIME correct dans la stdlib | `emersion/go-message` |
| Le support OpenAPI 3.1 est partiel dans oapi-codegen | voir le point 2.13 |

**Langages secondaires, inévitables mais à garder petits** : du **Lua** pour la politique KumoMTA (une politique fixe qui lit des données générées, sans logique métier) et du **TypeScript/SvelteKit** pour la console, statique, à partir de la v0.5.

---

## 2. Problèmes du document à régler avant de coder

Ces points sont soit des trous, soit des contradictions internes. Chacun est accompagné de la solution proposée.

### 2.1 Le corps des messages n'est stocké nulle part

Le schéma n'a pas de table pour le MIME. Or `dispatch` en a besoin pour injecter, et `postier recover` en a besoin pour réinjecter.

→ Ajouter une table `message_contents (content_id, tenant_id, created_at, mime bytea compressé en zstd, sha256)`. Le contenu est partagé par tous les destinataires d'un batch quand le rendu est identique. Il est purgé **à l'état terminal** du dernier destinataire, pas à l'injection, sinon la reprise après perte de spool devient impossible. La rétention « 0 jour » signifie donc « purge au statut terminal ». Le tout passe derrière une interface `BlobStore`, pour pouvoir déporter vers S3 au-delà d'un seuil (25 Mo répliqués de façon synchrone dans le WAL, ça pèse).

### 2.2 Recherche par `id` sur des tables partitionnées

La clé primaire est `(id, created_at)`. `GET /v1/emails/{id}` ne connaît que l'`id` et parcourrait donc toutes les partitions.

→ **Déduire `created_at` du timestamp contenu dans l'UUIDv7**, en générant l'UUID dans l'application et en posant `created_at` à partir de lui. Chaque requête par identifiant cible alors directement la bonne partition. On fait de même pour `events`.

### 2.3 La déduplication sur une table partitionnée

Une contrainte d'unicité sur une table partitionnée doit inclure la clé de partition. `ON CONFLICT (dedup_key)` ne compile donc pas : il faut écrire `ON CONFLICT (dedup_key, occurred_at)`. Cela ne fonctionne que si un log record rejoué porte exactement le même `occurred_at`, ce qui est le cas puisque ce timestamp vient du record lui-même.

→ `dedup_key = sha256(kumo_spool_id ‖ record_type ‖ timestamp ‖ num_attempts ‖ recipient)`. Sa stabilité est à valider en phase 0 sur de vrais records.

### 2.4 Le rate limit « quota ÷ nombre de replicas »

Les pods ne parlent pas à l'API Kubernetes et ne connaissent donc pas le nombre de replicas, qui change en plus avec le HPA. Par ailleurs, un compteur journalier par tenant incrémenté à chaque envoi crée une **ligne chaude**.

→ **Jetons loués** : chaque pod réserve un lot de jetons par tenant dans Postgres (`UPDATE … RETURNING`, par exemple 100 jetons) et les consomme en mémoire. C'est précis à un lot près, sans Redis et sans contention. Le même mécanisme sert au quota journalier.

### 2.5 La RLS face aux rôles transverses

`ingest`, `dispatch` et `webhooks` traitent des lots qui mélangent plusieurs tenants : `SET LOCAL postier.tenant_id` ne leur convient pas.

→ **Deux rôles Postgres.** `postier_app`, soumis à la RLS, sert à `api`, `smtp` et `tracking`. `postier_system`, avec `BYPASSRLS`, sert aux workers et à la synchronisation de l'opérateur. Le type Go `TenantTx` n'est constructible qu'avec un `tenant_id`, ce qui empêche à la compilation d'utiliser le mauvais pool.

→ **PgBouncer en mode transaction et pgx** : pgx prépare ses requêtes par défaut. Il faut soit PgBouncer ≥ 1.21 avec `max_prepared_statements`, soit `QueryExecModeCacheDescribe`.

### 2.6 Le double envoi lors de l'injection

Si `dispatch` injecte avec succès puis tombe avant de marquer le job terminé, le job est rejoué et l'email part deux fois. KumoMTA n'a pas d'idempotence native.

→ Lors d'un retry (tentative > 1), vérifier d'abord si un événement `queued` est arrivé pour ce `message_id`. S'il est là, on n'injecte pas ; sinon on attend le délai du log hook avant de réinjecter. Il restera un risque résiduel, à documenter comme livraison au moins une fois.

### 2.7 Les faux bounces : VERP non signé

`b+<id>@bounce.…` est forgeable : un attaquant envoie un faux DSN et fait supprimer une adresse. Un UUIDv7 est en partie prévisible, puisqu'il contient un timestamp.

→ `b+<id>.<hmac tronqué à 8 caractères>@bounce.<domaine>`, vérifié par `ingest`. Même principe pour le `Message-ID` (`<id.hmac@domaine>`), qui sert à rattacher les rapports ARF : Microsoft JMRP masque une partie du contenu.

### 2.8 SSRF sur les webhooks

Les URLs viennent des tenants : sans garde-fou, un webhook peut viser `169.254.169.254` ou des Services internes au cluster.

→ Un dialer qui résout le nom puis **refuse les plages privées, link-local et le CIDR du cluster**, et qui se connecte à l'IP validée (contre le DNS rebinding). Pas de redirections suivies. Une `NetworkPolicy` d'egress qui interdit le cluster aux pods `webhooks`. Validation de l'URL à l'admission.

### 2.9 Isolation par endpoint et licence de River

Plafonner la concurrence par endpoint demande la fonction de concurrence par clé de **River Pro**, qui est propriétaire et donc à proscrire dans un projet AGPL.

→ **River pour l'injection, les jobs planifiés et périodiques. Une file maison pour les webhooks**, avec `webhook_deliveries` en `SKIP LOCKED` : sélection équitable entre endpoints (round-robin sur ceux qui ont du travail) et bail de concurrence par endpoint en base.

### 2.10 Le flux `/v1/events?cursor=` perd des événements

Un curseur sur `occurred_at` rate les événements arrivés en retard. Un curseur sur un `bigserial` rate ceux dont la transaction commit dans le désordre, le problème classique de l'outbox.

→ Une colonne `seq bigserial` et une **lecture bornée à l'horizon sûr** : on ne sert que les `seq` dont la transaction est plus ancienne que `pg_snapshot_xmin(pg_current_snapshot())`. On stocke le `xid` à l'insertion, ou à défaut on applique un délai de garde de quelques secondes.

### 2.11 Rétention par tenant contre partitions globales

Détacher une partition mensuelle applique la même rétention à tous les tenants, ce qui contredit « réglable par `Tenant` ».

→ Partitions **hebdomadaires**. On les détache selon la rétention **maximale** des tenants, et un job en tâche de fond supprime par petits lots les données des tenants dont la rétention est plus courte. Les partitions futures sont créées par un job périodique River, sans dépendre de pg_partman.

### 2.12 La config KumoMTA dans un ConfigMap et des Secrets montés

- Un ConfigMap et un Secret sont limités à **1 Mio** chacun, soit environ 500 clés DKIM RSA par Secret.
- Monter un Secret par domaine modifie la spec du pod, donc **chaque nouveau domaine redémarre le MTA**.

→ Une politique Lua **fixe** qui relit ses fichiers de données avec `kumo.memoize` et une durée de cache de 60 s. Les volumes montés sans `subPath` se mettent à jour sans redémarrage, en 1 à 2 minutes environ. Les clés DKIM vont dans N Secrets agrégés, montés dès le départ. En v1, quand le nombre de domaines grandit, la politique récupérera ses données auprès d'un endpoint interne de Postier en mTLS, toujours via `memoize`. Ce comportement est à valider en phase 0.

### 2.13 OpenAPI 3.1 et la génération de code

oapi-codegen et kin-openapi gèrent mal la 3.1.

→ Écrire la spec en **3.0.3**, quitte à passer en 3.1 quand l'outillage suivra. SDK : `openapi-typescript` + `openapi-fetch` pour TypeScript, `openapi-python-client` pour Python, oapi-codegen en mode client pour Go. Éviter openapi-generator, dont le code produit est de mauvaise qualité.

### 2.14 Ce qui manque dans le modèle

- **Templates** : il n'y a ni table ni endpoint. → `templates` + `template_versions` immuables, et chaque message référence la version utilisée. Voir la décision D1.
- **Projets** : ils sont partout dans le schéma, mais aucune CRD ne les crée. → Voir la décision D2.
- **Endpoint `/v1/stats`** : agréger sur `messages` à chaque appel coûterait trop cher. → Table `stats_hourly` alimentée par un job incrémental qui lit `events` à partir d'un repère, toutes les minutes.
- **Logs et RGPD** : si `slog` écrit les adresses en clair, l'effacement devient impossible. → Ne logger que le hash de l'adresse. La politique de rétention des sauvegardes Barman est à documenter.

---

## 3. Architecture du code

C'est un **monolithe modulaire**. Le domaine ne dépend ni de HTTP, ni de SMTP, ni de Kubernetes.

```text
internal/
  domain/        # types purs : Message, Status, transitions, Event, Suppression (aucune I/O)
  accept/        # pipeline commun API+SMTP : auth → quota → domaine → suppression → rendu → tx(messages+content+job)
  render/        # html/template + text/template, mrml-wasm, construction MIME (go-message)
  store/         # sqlc, pools app/system, TenantTx, partitions
  queue/         # River (injection, planifiés, périodiques) + file équitable webhooks
  kumo/          # client d'injection, décodage des log records, rendu des données de politique
  ingest/  dispatch/  webhooks/  smtpd/  httpapi/  tracking/
  controller/    # réconciliateurs
  dnscheck/      # résolution directe des serveurs autoritaires (miekg/dns), DNSBL (Spamhaus DQS)
  platform/      # config (env + flags), slog, OTel, métriques, probes, arrêt propre
```

Règles :

- `domain/status.go` contient la **matrice de transitions explicite** et la fonction `AllowedFrom(to Status) []Status`, utilisée telle quelle dans le `UPDATE … WHERE status = ANY($1)`. Elle est testée de façon exhaustive et par propriétés : aucune séquence d'événements ne doit pouvoir faire régresser un statut.
- **L'idempotence s'insère dans la même transaction que le message.** On fait `INSERT idempotency_keys … ON CONFLICT DO NOTHING` en premier : une seconde requête concurrente bloque sur l'index unique jusqu'au commit de la première, puis rejoue sa réponse, ou renvoie `409` si le hash du corps diffère.
- **Re-vérifications dans `dispatch`** : juste avant d'injecter, `dispatch` relit le statut (le message peut avoir été annulé), la liste de suppression (une suppression a pu arriver entre-temps) et l'état du domaine.
- Les caches en mémoire (clés API vérifiées, état des domaines, suppressions chaudes) ont une durée de vie courte et sont invalidés par `LISTEN/NOTIFY`.

---

## 4. Phases

Le découpage suit la [feuille de route du document d'architecture](architecture.md#feuille-de-route-et-risques) : quatre phases, séparées par trois jalons. La feuille de route ne place pas tout ; chaque élément qu'elle ne mentionne pas est rattaché à la phase dont le jalon en dépend, et c'est signalé par *(hors feuille de route)*.

### Phase 0 — PoC : un tenant, une IP

Objectif : un envoi réel de bout en bout, et lever les inconnues de l'intégration KumoMTA avant de figer le schéma.

1. **Banc d'essai** : docker-compose avec KumoMTA épinglé, Postgres 16, Mailpit et un **sink SMTP programmable écrit en Go** (`test/smtpsink` : `550@…` provoque un 550, `421@…` un différé, et il peut générer de faux DSN asynchrones). Mailpit ne sait pas simuler de rejets.
2. **Capturer de vrais log records** pour chaque type (`Reception`, `TransientFailure`, `Delivery`, `Bounce`, `OOB`, `Feedback`, `Expiration`, `AdminBounce`, `Rejection`) et les figer en fichiers de référence pour les tests.
3. **Valider** : le regroupement en lots et les retries du log hook, la stabilité de `dedup_key`, le rechargement par `memoize` après modification d'un fichier monté, l'injection par destinataire contre l'injection groupée, les métadonnées présentes dans les records `OOB`.
4. **Socle** : `cmd/postier` avec ses sous-commandes, `platform/`, migrations goose en expand/contract, partitions, pools `app` et `system`, CI (golangci-lint, `go test -race`, govulncheck, contrôle des en-têtes SPDX avec REUSE).
5. **Domaine** : statuts et transitions, UUIDv7, normalisation des adresses (IDN en punycode, minuscules, NFC) et calcul de leur hash.
6. **API REST + outbox** : spec OpenAPI 3.0.3, `POST /v1/emails` avec corps HTML/texte fourni par le client, `GET /v1/emails/{id}` et `/events`, idempotence, erreurs RFC 9457, authentification Bearer (préfixe + SHA-256 + comparaison en temps constant), construction MIME (`Message-ID` signé), insertion transactionnelle du job River.
7. **`dispatch`** : workers River, injection avec keep-alive et concurrence bornée, garde-fou contre le double envoi.
8. **KumoMTA en `hostNetwork`** : politique Lua fixe qui lit ses données générées, listener d'injection HTTP authentifié, log hook avec les métadonnées `postier_id`, `tenant` et `trace`, VERP signé, listener entrant pour les bounces. Clé DKIM et domaine configurés à la main à ce stade.
9. **`ingest` + statuts** : décodage typé des records (testé sur les fichiers de référence et par fuzzing), transaction par lot, préséance, acquittement après commit, vérification du HMAC VERP.
10. **Chart Helm minimal** qui déploie les rôles, KumoMTA et Postgres, et un profil **quickstart sur kind** avec Mailpit comme destination.

**Jalon 1 — DKIM pass Gmail et Outlook, statuts exacts** : un email réel délivré en boîte Gmail et Outlook avec SPF, DKIM et DMARC alignés. Sur 10 000 messages vers le sink avec un mélange de réponses, les statuts sont cohérents à 100 %, sans perte ni doublon d'événement, même en tuant `ingest` puis `dispatch` pendant l'envoi.

### v0.1 — MVP

1. **Soumission SMTP** (`smtpd`) : 587 avec STARTTLS et 465 en TLS implicite, rechargement à chaud des certificats cert-manager, **PROXY protocol** (`pires/go-proxyproto`) pour connaître l'IP réelle derrière le LoadBalancer, limitation des tentatives d'AUTH par IP, budget mémoire global (sémaphore en octets × taille max), extraction puis retrait des en-têtes `X-Postier-*`.
2. **CRD et opérateur** : `SendingDomain`, `ApiCredential`, `WebhookEndpoint`, rendu des données KumoMTA, finalizers, synchronisation vers la base. Le chart Helm continue de déployer les rôles : `PostierCluster` vient en v0.5.
3. **Vérification DNS et DKIM** : génération des clés, conditions de statut, résolution directe des serveurs autoritaires, mode `external-dns` via `DNSEndpoint`, rotation (publication, attente de propagation, bascule, retrait à J+7) *(hors feuille de route)*. Un domaine non vérifié ne peut pas envoyer.
4. **Suppressions** : alimentation par `ingest` (bounces durs, plaintes, bounces temporaires répétés), `/v1/suppressions`, vérification à l'acceptation et re-vérification dans `dispatch`.
5. **Webhooks signés** : Standard Webhooks, file équitable, échéancier de retries, statut `dead` et rejeu, dialer anti-SSRF, désactivation après 3 jours d'échecs.
6. **Métriques et alertes** : les métriques prévues, le label `provider` dérivé des MX, `PrometheusRule`, `ServiceMonitor` et les trois dashboards.
7. **Compléments de l'API** : recherche par curseur, `/v1/events` (horizon sûr), job `stats_hourly`, quotas par jetons loués, templates `html/template` + `text/template` avec variables et plafonds de taille *(hors feuille de route ; MJML vient en v1.0)*.
8. **e2e sur kind** : envoi API et SMTP, bounce simulé, webhook reçu, redémarrage de KumoMTA.

**Jalon 2 — e2e verts, premier utilisateur externe** : tous les e2e passent en CI, et un premier utilisateur externe envoie en production depuis son propre domaine.

### v0.5 — Multi-tenant

1. **RLS + namespaces** : CRD `Tenant`, RLS activée, `ClusterRole postier-tenant-admin`, webhook d'admission (unicité des domaines doublée d'une contrainte unique en base pour fermer la course, pools autorisés), pause automatique sur taux de bounce ou de plainte.
2. **Egress `kumo-proxy`** : CRD `IPPool` en mode `proxy`, et `TrafficShapingPolicy` avec règles communautaires et profils des FAI français.
3. **Chauffe des IP** : `WarmupPlan`, plafonds par IP et par fournisseur, débordement vers les IP chaudes, état de chauffe exposé dans le statut de l'`IPPool`.
4. **Throttles partagés** : Valkey dès qu'il y a plus d'un nœud MTA, `tsa-daemon` avec un petit PVC.
5. **Tracking opt-in**, désactivé par défaut : URLs signées par HMAC (pas d'open redirect), marquage des ouvertures automatiques (Apple MPP, scanners), certificats par domaine de tracking.
6. **Console en lecture** : SvelteKit statique, servie par `api`.
7. **Cycle de vie du cluster** *(hors feuille de route, requis par le jalon 3)* : `PostierCluster` (l'opérateur déploie les rôles, le StatefulSet KumoMTA et CloudNativePG si demandé), scale-up et **drain** automatisés, `postier recover --node` et alerte `PostierOrphanedMessages`, traces OTel liées de l'API jusqu'à l'`ingest`.

**Jalon 3 — test de reprise réussi, charge mesurée** : bascule CloudNativePG et perte d'un nœud KumoMTA chronométrées, sans perte de message accepté ; débit et lag mesurés avec k6, et RPO/RTO publiés. Deux tenants isolés, avec un test qui prouve qu'un bug applicatif sans `tenant_id` ne lit rien.

### v1.0 — Stable

1. **API v1 figée** : contrat `/v1` gelé, CRD en `v1beta1` avec webhook de conversion.
2. **Compatibilité Resend** : préfixe `/compat/resend/`.
3. **Templates MJML** : compilation à l'enregistrement par mrml-wasm (wazero), `templates` + `template_versions` immuables.
4. **Envois programmés** : état `scheduled`, `send_at` porté par un job River planifié, `POST /v1/emails/{id}/cancel` avec re-vérification du statut dans `dispatch`.
5. **Audit de sécurité** : revue externe, chaos (pod KumoMTA tué en plein envoi), 72 h de charge soutenue ; `DELETE /v1/recipients/{address}` sur toutes les partitions et journal d'audit *(hors feuille de route)*.
6. **Doc et SDK** : SDK publiés sous Apache-2.0 ou MIT, documentation d'exploitation ; supply chain complète (build avec `ko` ou goreleaser, images distroless multi-arch, signature cosign keyless, SBOM, provenance SLSA, chart OCI sur GHCR, bot CLA).

---

## 5. Stratégie de test

| Niveau | Cible | Outil |
|---|---|---|
| Unitaire et propriétés | transitions, normalisation d'adresses, VERP/HMAC, décodage des records | `testing` + `rapid` |
| Fuzzing | parseur MIME SMTP, log records, URL de webhook | fuzzing natif de Go |
| Intégration Postgres | RLS, idempotence concurrente, `SKIP LOCKED`, horizon sûr, partitions | Testcontainers |
| Intégration KumoMTA | politique Lua, injection, log hook | Testcontainers + `smtpsink` |
| Opérateur | réconciliations, finalizers, admission | envtest |
| e2e | parcours complets | kind + Mailpit + `smtpsink` |
| Charge et chaos | débit, lag, drain | k6, Chaos Mesh |

---

## 6. Décisions à trancher

- **D1 — Templates** : API seule (dynamique, versionnée) ou CRD `EmailTemplate` (cohérent avec GitOps, mais limité à 1,5 Mo dans etcd et contraire à « les données dynamiques restent en base ») ? Recommandation : l'API, avec un import optionnel depuis Git en CLI.
- **D2 — Projets** : une CRD `Project`, ou bien projet = namespace avec un tenant regroupant plusieurs namespaces ? Recommandation : une CRD `Project` dans le namespace du tenant, référencée par `SendingDomain` et `ApiCredential`.
- **D3 — Stockage des corps** : Postgres seul jusqu'en v1, ou S3 dès le départ au-delà d'un seuil ? Recommandation : Postgres d'abord, derrière l'interface `BlobStore`.
- **D4 — Garantie d'injection** : accepter officiellement une livraison au moins une fois, avec le garde-fou du point 2.6, en le documentant.

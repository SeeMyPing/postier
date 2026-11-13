# Phase 0 : ce qui a été validé sur le vrai KumoMTA

Ce document consigne les réponses aux inconnues listées dans le [plan d'implémentation](implementation-plan.md#phase-0--poc--un-tenant-une-ip) (points 2 et 3 de la phase 0), les écarts que ces réponses ont imposés par rapport aux deux documents de conception, et ce qui reste à faire pour fermer le jalon 1.

Tout a été mesuré sur le banc d'essai local (`compose.yaml`) : KumoMTA `2026.09.29-b90d8bc1`, Postgres 16, `smtpsink`, Mailpit, dans une VM podman de 4 vCPU qui héberge tout. Les débits ne sont donc pas un benchmark : ils servent à repérer les goulets, pas à dimensionner.

## État du jalon 1

| Critère | État |
| --- | --- |
| Sur 10 000 messages vers le sink avec un mélange de réponses, statuts cohérents à 100 %, sans perte ni doublon d'événement, en tuant `ingest` puis `dispatch` pendant l'envoi | **Validé** (`make milestone`) |
| Un email réel délivré en boîte Gmail et Outlook avec SPF, DKIM et DMARC alignés | **Non fait** : demande un domaine, une IP publique avec PTR et le port 25 sortant |

Le second critère ne peut pas se vérifier sur un banc local. La signature DKIM, le return-path VERP et le `Message-ID` sont en place et vérifiés sur les messages reçus par le sink ; il manque l'essai en conditions réelles. La marche à suivre est dans le [README](../README.md#envoyer-un-email-réel).

### Le test des 10 000 messages

`make milestone` envoie 10 000 messages par l'API, tue `ingest` (SIGKILL) trois secondes après le début puis `dispatch` quatre secondes plus tard, les redémarre, puis vérifie **chaque message** par l'API : son statut final, son historique complet d'événements, et l'absence de double livraison côté sink.

| Issue simulée | Messages | Historique attendu | Exacts |
| --- | ---: | --- | ---: |
| Livré | 7 000 | accepted, queued, delivered | 7 000 |
| Rejet 550 au `RCPT TO` | 900 | accepted, queued, bounced | 900 |
| Rejet 554 après `DATA` | 400 | accepted, queued, bounced | 400 |
| Différé une fois, puis livré | 800 | accepted, queued, deferred, delivered | 800 |
| Bounce asynchrone (DSN) | 500 | accepted, queued, delivered, bounced | 500 |
| Plainte (ARF) | 300 | accepted, queued, delivered, complained | 300 |
| Différé jusqu'à expiration | 100 | accepted, queued, deferred…, expired | 100 |

Résultat : 10 000 sur 10 000, 8 600 livraisons au sink pour 8 600 attendues, 0 doublon. L'arrêt de `dispatch` a interrompu 30 jobs d'injection en plein vol ; à leur reprise, le garde-fou en a écarté 24 dont le message était déjà parti, et les 6 autres ont été injectés pour la première fois.

L'arrêt d'`ingest` ne suffit pas à éprouver la déduplication : KumoMTA échoue à se connecter et ne renvoie rien en double. Un second passage force donc de vraies redites, avec `ingest --chaos-drop-ack 0.3`, qui répond `503` à 30 % des lots **après** les avoir commités. Sur 3 000 messages : 998 lots renvoyés par KumoMTA, 3 158 records en double absorbés, statuts et historiques exacts, aucun événement dupliqué en base.

## Réponses aux inconnues

### Les neuf types de log records

Un record réel de chaque type est figé dans [`internal/kumo/testdata/records`](../internal/kumo/testdata/records), et une session complète de 27 records dans `session.json`. Les tests du décodeur et du mapping des statuts tournent sur ces fichiers, jamais sur des records écrits d'après la documentation. Ils se régénèrent avec `ingest --capture-dir` puis `make capture` à chaque montée de version de KumoMTA : le diff est la revue de ce que l'amont a changé.

Ce que les records réels ont appris, et que la documentation ne dit pas :

| Constat | Conséquence |
| --- | --- |
| Un record `OOB` n'a **aucune métadonnée**. Son champ `sender` contient l'adresse VERP (le destinataire d'enveloppe du bounce), son champ `recipient` le destinataire d'origine. | `ingest` rattache le bounce par le VERP, après vérification du HMAC. C'est la réponse à « les métadonnées présentes dans les records OOB » : aucune, et il n'en faut pas. |
| L'`event_time` d'un record `OOB` ou `Feedback` est recopié du rapport (`Arrival-Date`), donc **choisi par l'expéditeur du bounce**. | `occurred_at` est la clé de partition : un bounce daté de 2016 ou de 2040 viserait une partition inexistante et bloquerait son lot. Pour ces deux types, Postier prend l'heure de réception par KumoMTA (`created_time`) et garde la date annoncée dans le détail de l'événement. |
| Un record `Feedback` cite le message d'origine, `Message-ID` compris, et donne `original_mail_from` quand le rapport le contient. | Rattachement par le `Message-ID` signé d'abord (il survit au masquage des adresses), par le VERP sinon. |
| Un record `Rejection` concerne un refus du SMTP **entrant**. Il n'a ni identifiant de spool ni métadonnées. | Il ne correspond à aucun message Postier et est ignoré. Le statut `rejected` vient d'ailleurs, voir ci-dessous. |
| Un refus de la politique à l'injection HTTP revient en **HTTP 200**, avec `fail_count: 1` et une erreur du type `adresse: 550 5.7.1 raison`, suivie d'une trace Lua. | Le client d'injection lit le code : 5xx est un rejet définitif, 4xx un échec à rejouer. C'est `dispatch` qui pose le statut `rejected`, de façon synchrone. |
| Un `AdminBounce` est classé `AuthenticationFailed` par KumoMTA, à cause du code 5.7.1 qu'il fabrique. | La classification n'est pas reprise pour l'événement `canceled`. |
| Le contenu injecté est traité comme un **template Jinja** par défaut. | Un `{{ … }}` dans le HTML d'un client serait interprété. L'injection passe `template_dialect: "Static"` (disponible depuis 2025.12). |
| KumoMTA ajoute par défaut un en-tête `X-KumoRef` aux messages injectés en HTTP. | Désactivé : il n'est pas signé, et Postier rattache les plaintes par son propre `Message-ID`. |
| L'API d'injection accepte des métadonnées par destinataire, rangées sous la clé `extra` (depuis 2026.05). | Pas d'en-têtes `X-Postier-*` à ajouter puis à retirer : `dispatch` passe l'identifiant, le tenant et la trace en métadonnées, la politique les promeut en clés journalisées. |

### Regroupement en lots et retries du log hook

- **Lots.** KumoMTA n'attend pas pour remplir un lot : il envoie ce qui est prêt. En régime normal, la taille médiane d'un lot est 1, le 90ᵉ centile 3. Les lots ne grossissent qu'en cas de retard (jusqu'à 67 records après le redémarrage d'`ingest`). Deux réglages existent côté KumoMTA, `min_batch_size` et `max_batch_latency`, exposés dans le fichier de données (`log_hook`) mais laissés vides : à activer quand le coût d'une transaction par record deviendra mesurable.
- **Retries.** Tout échec du hook (connexion refusée, `503`) est traité comme transitoire : les records restent dans la file durable de KumoMTA et sont renvoyés. La politique ne renvoie jamais de `5xx` au moteur, qui supprimerait le record. File réglée à 5 s d'intervalle, 7 jours de durée de vie.
- **Record indécodable.** Un record qu'`ingest` ne sait pas lire est rangé dans la table `kumo_dead_records`, dans la même transaction que le reste du lot. Il ne bloque pas ses voisins et n'est pas perdu.

### Stabilité de `dedup_key`

`dedup_key = sha256(id de spool ‖ type ‖ event_time ‖ num_attempts ‖ destinataire)`, avec `event_time` à la nanoseconde.

- Sur la session de référence, 27 records donnent 27 clés distinctes.
- Un lot renvoyé porte des records identiques octet pour octet : 3 158 redites réelles absorbées, voir plus haut.
- Chaque champ est nécessaire : un message produit plusieurs `TransientFailure` (un par tentative), et un DSN produit un record `OOB` par destinataire en échec, tous avec le même identifiant de spool.

La contrainte d'unicité est `(dedup_key, occurred_at)`, comme prévu au point 2.3 du plan.

### Rechargement par `memoize`

Ajout d'un domaine d'envoi au fichier de données, KumoMTA en marche : le message en attente pour ce domaine a été livré 14 secondes plus tard, signé avec la nouvelle clé DKIM, **sans redémarrage** (heure de démarrage du conteneur inchangée). Entre-temps, la politique répondait `451` et `dispatch` rejouait.

Le délai se décompose en durée de cache (5 s sur le banc, 60 s par défaut) et en backoff de `dispatch`.

Sur Kubernetes s'y ajoute la synchronisation du ConfigMap par le kubelet. Mesuré sur kind avec le chart : après un `helm upgrade` qui ne changeait que le fichier de données (une route), le message en attente a été livré **78 secondes** plus tard, le pod KumoMTA gardant la même heure de démarrage et zéro redémarrage. Le chart monte le fichier sans `subPath`, condition pour que le kubelet le mette à jour, et l'exclut du checksum qui redémarre le pod quand la politique Lua change.

`relay_hosts` ne peut pas recevoir une liste vide depuis Lua (une table vide est ambiguë entre liste et dictionnaire). L'interdiction de relayer est donc posée dans `smtp_server_rcpt_to`, qui refuse tout destinataire hors des domaines de return-path et de feedback.

### Injection par destinataire ou groupée

**Par destinataire.** L'API d'injection ne prend qu'un `envelope_sender` par requête, or chaque destinataire a son VERP et son `Message-ID`. Grouper demanderait de calculer le HMAC en Lua, donc de mettre la clé de signature et de la logique dans la politique, contre le principe « une politique fixe, sans logique métier ».

Le coût est une requête HTTP par message, sur des connexions gardées ouvertes. Le corps reste stocké une seule fois par envoi : il est rendu sans `Message-ID`, que `dispatch` ajoute en tête au moment d'injecter.

Mesures : l'API accepte 800 à 1 300 messages par seconde. L'injection plafonnait à 150 par seconde, à cause du délai minimal de River entre deux lectures de la file (100 ms, pour 16 workers au plus par lecture) et non de KumoMTA. Avec 25 ms et 32 workers, elle atteint 200 par seconde en médiane et 317 en pointe sur le banc, où la limite devient le processeur de la VM.

### Ce que le déploiement sur kind a appris

Le chart a été installé sur un cluster kind (profil `values-quickstart.yaml`) et un email envoyé de bout en bout jusqu'à Mailpit. Trois points ne se voyaient pas sur le banc compose :

- **Le résolveur de KumoMTA n'applique pas les domaines de recherche DNS du pod.** Un nom court de Service ne résout pas (`451 MX didn't resolve to any hosts`). Une route vers un Service doit porter son nom complet, terminé par un point ; le chart le génère pour Mailpit.
- **KumoMTA abandonne ses privilèges lui-même** et garde `NET_BIND_SERVICE`. Avec toutes les capacités retirées, il lui faut `SETUID`, `SETGID`, `SETPCAP` et `NET_BIND_SERVICE`, sinon `capset` échoue au démarrage. C'est un écart avec le document d'architecture, qui voulait éviter `NET_BIND_SERVICE` hors `hostNetwork` : l'image amont l'exige dans tous les cas.
- **Un StatefulSet ne remplace pas seul un pod en échec** lors d'une mise à jour. Après une correction de configuration, il faut supprimer le pod KumoMTA à la main. L'opérateur devra le gérer.

Le mode `hostNetwork`, valeur par défaut du chart, n'a été vérifié qu'au rendu des manifestes : kind n'a pas d'IP publique à lui donner.

## Écarts par rapport aux documents de conception

| Sujet | Document | Implémenté | Raison |
| --- | --- | --- | --- |
| Transitions de statut | Seul `delivered` peut encore changer | `complained` est accepté depuis tout état antérieur, et remplace aussi `bounced` | L'ordre des records n'est pas garanti. Sans cela, le statut final dépendait de l'ordre d'arrivée d'une plainte et de sa livraison. Vérifié par propriété sur la session réelle, dans tout ordre. |
| Transitions de statut | `queued → delivered` | `accepted` peut sauter à tout état ultérieur | Un `Delivery` peut précéder le `Reception` du même message. |
| Compteur de tentatives | Mis à jour avec le statut | Monte même quand le statut ne bouge pas | Trouvé par le test d'ordre inversé : un bounce asynchrone traité avant la livraison laissait le compteur à zéro. |
| Statut `rejected` | Produit par le record `Rejection` | Posé par `dispatch` au refus d'injection | Voir le tableau des constats. |
| Tests d'intégration | Testcontainers | Postgres désigné par `POSTIER_TEST_DATABASE_URL` | Même couverture, sans dépendre d'un socket Docker ; un service en CI, `make test-integration` en local. |
| Row-level security | Deux rôles Postgres | Les deux rôles et leurs droits sont en place et testés ; les politiques RLS viennent en v0.5, comme prévu | `TenantTx` positionne déjà `postier.tenant_id`. |
| Chart Helm | Installe l'opérateur | Déploie les rôles directement | Conforme au plan : l'opérateur arrive en v0.1, `PostierCluster` en v0.5. |
| Capacités de KumoMTA | Pas de `NET_BIND_SERVICE` hors `hostNetwork` | Toujours accordée, avec `SETUID`, `SETGID`, `SETPCAP` | Exigée par l'image amont pour abandonner ses privilèges. |

## Garantie d'injection (décision D4)

La livraison est **au moins une fois**. KumoMTA n'a pas d'injection idempotente : si `dispatch` meurt entre l'acceptation du message par KumoMTA et la fin du job, le job repart.

Garde-fou : à partir de la deuxième tentative, `dispatch` cherche un événement venu de KumoMTA pour ce message. S'il existe, il n'injecte pas. Sinon, et si la tentative précédente date de moins de `--guard-delay` (15 s), il attend ce délai avant de réinjecter.

Risque résiduel : un double envoi reste possible si le log hook a plus de retard que le délai de garde au moment de la reprise, par exemple quand `ingest` est indisponible pendant qu'un pod `dispatch` est tué. Dans le test du jalon, les deux pannes se chevauchaient et aucun doublon n'est apparu, parce que la reprise d'un job orphelin attend deux minutes. Ce n'est pas une preuve générale.

## Ce qui n'est pas dans la phase 0

- Suppressions, webhooks, SMTP de soumission, CRD et opérateur, vérification DNS : v0.1.
- Métriques Prometheus et traces OpenTelemetry. L'identifiant de trace voyage déjà de l'API jusqu'aux records, il n'est pas encore relié à des spans.
- Purge des corps de messages à l'état terminal : la table et l'encodage existent, le job de purge non.
- `NetworkPolicy` dans le chart.

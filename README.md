# Postier

Plateforme d'email transactionnel auto-hébergée, native Kubernetes. Postier confie la livraison SMTP à [KumoMTA](https://kumomta.com) et apporte ce qui lui manque : une API, un état par message, et bientôt les suppressions, les webhooks et un opérateur.

> **État : phase 0 (PoC), un tenant, une IP.** Le chemin complet fonctionne : API REST, outbox, injection, livraison, retour des événements, statuts. Ce n'est pas prêt pour la production. Voir [ce qui a été validé](docs/validation-phase0.md) et la [feuille de route](docs/implementation-plan.md#4-phases).

- [Architecture](docs/architecture.md)
- [Plan d'implémentation](docs/implementation-plan.md)
- [Validation de la phase 0](docs/validation-phase0.md) : ce qui a été mesuré sur le vrai KumoMTA, et les écarts qui en découlent

## Comment ça marche

```text
client ──► postier api ──► Postgres ──► postier dispatch ──► KumoMTA ──► MX destinataire
                              ▲                                 │
                              └──────── postier ingest ◄────────┘  log records
```

1. `postier api` valide la requête et écrit, dans une seule transaction, un message par destinataire, son événement `accepted` et son job d'injection. Le `202` ne part qu'après le commit.
2. `postier dispatch` prend les jobs et injecte dans KumoMTA, avec un return-path et un `Message-ID` signés.
3. KumoMTA signe en DKIM et livre. Chaque étape produit un log record, envoyé à `postier ingest`.
4. `postier ingest` transforme les records en événements et fait avancer le statut. Un statut ne recule jamais, quel que soit l'ordre d'arrivée des records.

Postgres porte tout l'état. Les trois rôles sont sans état et se remplacent sans perte.

## Essayer en local

Il faut Go 1.27, et Docker ou Podman avec compose.

```bash
make bench-up
```

Le banc démarre Postgres, KumoMTA, les trois rôles, Mailpit et `smtpsink`. Rien n'en sort : toutes les destinations sont routées vers Mailpit, et `*@sink.test` vers `smtpsink`.

Envoyer un email (la clé est une valeur de développement publique, définie dans `compose.yaml`) :

```bash
curl -X POST http://localhost:8080/v1/emails -H "Authorization: Bearer pst_test_siyd5ddj_E8h8Jtb0LlgMfGZXzBmgyeX5diTL9iQQraCQV9EDJbU" -H "Content-Type: application/json" -d '{"from":"Acme <no-reply@acme.test>","to":["vous@example.org"],"subject":"Bonjour","html":"<p>Ça marche.</p>"}'
```

Le message apparaît dans Mailpit, sur <http://localhost:8025>. La réponse donne un identifiant par destinataire, à suivre avec `GET /v1/emails/{id}` et `GET /v1/emails/{id}/events`. La spec OpenAPI est servie sur <http://localhost:8080/openapi.yaml>.

### Simuler des échecs

`smtpsink` décide de sa réponse d'après la partie locale du destinataire :

| Destinataire | Effet | Statut final |
| --- | --- | --- |
| `ok@sink.test` | accepté | `delivered` |
| `550@sink.test` | rejet 550 au `RCPT TO` | `bounced` |
| `data-554@sink.test` | rejet 554 après `DATA` | `bounced` |
| `defer2-x@sink.test` | 451 deux fois, puis accepté | `delivered` |
| `421@sink.test` | 421 à chaque essai | `expired` |
| `oob-x@sink.test` | accepté, puis bounce asynchrone (DSN) | `bounced` |
| `fbl-x@sink.test` | accepté, puis plainte (ARF) | `complained` |

Ses compteurs sont sur <http://localhost:8026/stats>.

## Sur Kubernetes

Sur un cluster kind local, avec Mailpit comme destination :

```bash
make quickstart
```

Le script crée le cluster, charge l'image, génère une clé DKIM, installe le chart avec le profil `values-quickstart.yaml` et affiche la clé d'API. Il n'écrit pas dans votre kubeconfig : les accès du cluster sont dans `.quickstart/kubeconfig`.

Le chart [`deploy/helm/postier`](deploy/helm/postier) déploie les rôles, KumoMTA et, par défaut, un Postgres à une instance sans sauvegarde, suffisant pour essayer. Par défaut KumoMTA tourne en `hostNetwork` sur un nœud dédié (label et taint `postier.dev/mta`), le mode d'egress simple pour une IP.

### Envoyer un email réel

C'est la partie du jalon 1 qui ne se vérifie pas en local. Il faut un domaine, une IP publique dont le PTR correspond au nom EHLO, et le port 25 sortant ouvert.

1. Générer la clé DKIM et publier l'enregistrement affiché :

   ```bash
   postier dkim --domain notifications.exemple.fr --selector pst1 --out exemple.key
   ```

2. Créer le Secret attendu par le chart :

   ```bash
   kubectl create secret generic postier-dkim --from-file=notifications.exemple.fr.pst1.key=exemple.key
   ```

3. Publier le reste du DNS : MX de `bounce.notifications.exemple.fr` vers le nœud KumoMTA, SPF sur ce même sous-domaine, et un enregistrement `_dmarc`.
4. Installer le chart avec `domains`, `kumomta.hostname` et `seed.enabled=true`, sans `kumomta.routes`.

## Développement

```bash
make help              # toutes les cibles
make test              # tests unitaires, avec le détecteur de courses
make test-integration  # idem, plus les tests sur un vrai Postgres
make lint              # golangci-lint
make generate          # sqlc, OpenAPI, copie de la politique dans le chart
make milestone         # 10 000 messages pendant qu'ingest et dispatch sont tués
```

Sans `POSTIER_TEST_DATABASE_URL`, les tests d'intégration sont ignorés : `make test` ne couvre donc ni l'idempotence concurrente, ni les droits des rôles Postgres, ni le rejeu de la session réelle en base.

### Organisation

```text
cmd/postier/         le binaire : api | dispatch | ingest | migrate | seed | dkim
internal/domain/     types purs : statuts et transitions, identifiants, adresses, signatures
internal/accept/     pipeline d'acceptation : auth, validation, idempotence, outbox
internal/render/     construction MIME
internal/store/      Postgres : pools app et system, requêtes sqlc, migrations
internal/queue/      River : injection et tâches périodiques
internal/kumo/       tout ce qui dépend de KumoMTA : client, log records, fichier de données
internal/dispatch/   injection, garde-fou contre le double envoi
internal/ingest/     log records vers événements et statuts
internal/httpapi/    API REST, générée depuis openapi/postier.yaml
kumo/policy/         politique Lua de KumoMTA, fixe
db/                  migrations et requêtes SQL
deploy/              chart Helm, banc compose
test/                smtpsink, test du jalon, capture des records de référence
```

Le format des log records de KumoMTA est testé sur des records réels, capturés sur la version épinglée (`internal/kumo/testdata`). À chaque montée de version : relancer la capture avec `postier ingest --capture-dir`, puis `make capture DIR=…`, et relire le diff.

## Licence

Le serveur est sous [AGPL-3.0-only](LICENSE). Chaque fichier porte son en-tête SPDX, vérifié par [REUSE](https://reuse.software) en CI. Les SDK et les exemples seront publiés sous licence permissive.

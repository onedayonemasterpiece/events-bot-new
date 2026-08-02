# INC-2026-07-29-mtproto-proxy-desktop-disconnect Telegram Desktop lost MTProto proxy connection

Status: closed
Severity: sev3
Service: host-level Telegram MTProto proxy (`vpn-server-mtproto-1`)
Opened: 2026-07-29
Reopened: 2026-07-31
Closed: 2026-07-31
Owners: server operator
Related incidents: —
Related docs: `/home/dev/projects/vpn-server/docs/operations.md`

## Summary

Пользователь сообщил, что Telegram Desktop потерял соединение через MTProto
proxy на сервере. Серверная диагностика не подтвердила полный outage: контейнер
работал, TCP `1443` слушал, существовали внешние клиентские соединения и
успешные handshake с Telegram DC. Для сброса возможного stale client/server
state был кратко пересоздан только MTProto-контейнер. Одновременно устранено
избыточное хранение его application logs. При повторении 2026-07-31 серверный
outage снова не подтвердился: Desktop доходил до proxy, но использовал
сохранённый fake-TLS профиль, а новую DD-ссылку с тем же IP и портом интерфейс
не добавлял. Ссылка с DNS-алиасом того же IP обошла дедупликацию профиля,
после чего Desktop восстановил соединение.

## User / Business Impact

- Telegram Desktop пользователя отображал отключённый proxy.
- Другие клиенты продолжали подключаться, поэтому blast radius полного outage
  не подтверждён.
- Root filesystem был заполнен на 91% перед работами; MTProto-контейнер держал
  около 700 MiB старых ротаций application log.

## Detection

- Первичный сигнал: сообщение пользователя.
- Серверные проверки: Docker state, listener `1443`, established downstream
  sockets, MTProto application log и успешные Telegram DC handshakes.
- Пробел observability: у контейнера не было healthcheck, а application log
  хранился внутри writable layer с upstream default rotation 100 MiB x 10.

## Timeline

- 2026-07-29 18:52 UTC — начата диагностика хоста, Docker и диска.
- 2026-07-29 18:53 UTC — подтверждены listener `1443`, внешние downstream
  соединения и успешные Telegram DC handshakes.
- 2026-07-29 19:42 UTC — активный лог сохранён; MTProto-контейнер пересоздан
  за 3 секунды с persistent log mount и уменьшенным retention.
- 2026-07-29 19:44 UTC — подтверждены 93 established downstream соединения,
  новые Telegram DC handshakes и рабочий listener.
- 2026-07-29 19:58 UTC — подтверждено, что основной desktop peer достигает
  `1443` и отправляет 1452-byte payload, но proxy оставляет соединения в init и
  закрывает их по timeout.
- 2026-07-29 20:01 UTC — включён совместимый `mtp_secure`/DD protocol без смены
  базового секрета; бот начал выдавать отдельную ссылку `Telegram Desktop (DD)`.
- 2026-07-29 20:04 UTC — protocol-aware `mtp_ping` подтвердил DD path до
  Telegram DC 1/2/3: 3/3 OK.
- 2026-07-29 20:08 UTC — пользователь подтвердил успешное подключение Telegram
  Desktop 7.0.6 через новую DD-ссылку.
- 2026-07-31 06:09 UTC — пользователь сообщил о повторном зависании Telegram
  Desktop в состоянии подключения длительностью около 15 минут.
- 2026-07-31 06:11 UTC — подтверждено: контейнер работает без restart/OOM,
  `1443` слушает, Telegram DC handshakes успешны; packet capture входящего
  Desktop traffic показывает fake-TLS ClientHello (`16 03 01`), а не DD.
- 2026-07-31 06:23 UTC — Desktop не смог удалить старый proxy profile и
  игнорировал повторное добавление DD-ссылки с тем же IP и портом.
- 2026-07-31 06:24 UTC — выпущена эквивалентная DD-ссылка через DNS-алиас
  `188-227-84-107.sslip.io`, резолвящийся в тот же server IP, чтобы создать
  отдельную client-side proxy identity.
- 2026-07-31 06:25 UTC — пользователь подтвердил подключение. На сервере
  зафиксированы 31+ established sessions от Desktop peer, двусторонний traffic
  и свежие Telegram DC handshakes.
- 2026-07-31 06:26 UTC — удалена только старая rotation
  `application.log.0` от 2026-07-29 (48,703,203 bytes); активный лог
  `application.log` за 2026-07-31 сохранён.

## Root Cause

Полный server-side outage не подтверждён ни в первичном эпизоде, ни при
повторении. Proxy обслуживал клиентов и соединялся с Telegram DC. В обоих
эпизодах packet-level диагностика показала, что проблемный Telegram Desktop
открывает множество TCP connections и отправляет fake-TLS ClientHello, после
чего часть соединений остаётся в `init` до inactive timeout.

Повторение уточнило причину: Desktop хранил несколько proxy profiles для одного
`IP:port`, не удалял старую запись окончательно и игнорировал новую DD-ссылку с
тем же endpoint. После добавления эквивалентного endpoint через другое hostname
клиент создал отдельную запись и восстановил соединение; после восстановления
наблюдался устойчивый двусторонний fake-TLS application traffic. Поэтому
наиболее вероятная непосредственная причина user-visible outage — stale/colliding
client-side proxy profile state Telegram Desktop, а не отказ listener или
Telegram upstream. DD остаётся совместимым обходным путём, но одинаковый
`IP:port` не гарантирует, что Desktop сохранит его как отдельный профиль.

Отдельный подтверждённый эксплуатационный дефект: дефолтная ротация Erlang
logger позволяла накопить сотни MiB старых логов внутри container writable
layer. Это не было непосредственной причиной отключения, но увеличивало риск
будущего disk-pressure incident.

## Contributing Factors

- отсутствовал container healthcheck;
- application log не был вынесен в persistent host path;
- default retention был значительно выше необходимого;
- root filesystem уже имел повышенное заполнение.
- proxy image от 2026-04-12 принимал старый fake-TLS path, который текущий
  Desktop peer не смог завершить;
- proxy startup жёстко зависит от внешнего Telegram core-config HTTP fetch:
  при последнем recreate было пять timeout/restart попыток до успешного старта.
- Telegram Desktop дедуплицирует либо восстанавливает proxy profiles с
  одинаковым `IP:port`, скрывая различия их secret/protocol в списке.

## Automation Contract

### Treat as regression guard when

- меняется `vpn-server` MTProto service, его entrypoint, log mount, logger
  settings, порт или Docker logging options;
- расследуется новый Telegram proxy disconnect или disk pressure на этом хосте.

### Affected surfaces

- `/home/dev/projects/vpn-server/docker-compose.yml`;
- `/home/dev/projects/vpn-server/data/mtproto-logs`;
- container `vpn-server-mtproto-1`;
- TCP `1443`;
- root filesystem capacity.

### Mandatory checks before closure or deploy

- `docker compose config --quiet`;
- container state `running`, no OOM/restart loop;
- listener on `0.0.0.0:1443`;
- at least one fresh Telegram DC `handshake complete`;
- protocol-aware DD `mtp_ping` к нескольким Telegram DC;
- active/preserved current-day log still exists;
- logger cap is 25 MiB and two rotations;
- filesystem capacity is recorded before and after cleanup.

### Required evidence

- operational config path and validation result;
- container start timestamp and restart count;
- post-restart downstream count and Telegram DC handshake evidence;
- log inventory and disk usage.

## Immediate Mitigation

- Активный лог за текущий день сохранён на host path.
- Пересоздан только MTProto-контейнер; downtime составил около 3 секунд.
- Старые container-local rotations удалены вместе со старым writable layer.
- После подтверждения продолжающегося Desktop outage включён DD protocol и
  выпущена отдельная Desktop-ссылка без ротации базового секрета.

## Corrective Actions

- Добавлен persistent mount `data/mtproto-logs:/var/log/mtproto-proxy`.
- Erlang logger ограничен 25 MiB на файл и двумя ротациями.
- Docker json-file log ограничен двумя файлами по 10 MiB.
- Runbook дополнен retention и preserve-before-recreate инструкцией.
- MTProto одновременно разрешает `mtp_secure` (DD) и fake-TLS.
- VPN admin bot помечает DD link как рекомендуемый для Telegram Desktop, сохраняя
  fake-TLS links для уже работающих клиентов.
- Для восстановления Desktop при конфликте сохранённых профилей проверен
  отдельный DNS hostname, резолвящийся в тот же proxy IP.

## Follow-up Actions

- [ ] Добавить protocol-aware healthcheck/monitoring для MTProto.
- [ ] Убрать fail-hard startup dependency от Telegram core-config HTTP fetch
      либо добавить bounded cached fallback.
- [ ] Привязать текущий `vpn-server` checkout к remote и зафиксировать deployed
      Compose state отдельным воспроизводимым commit.
- [ ] Выделить DD для Desktop на стабильный отдельный hostname/endpoint вместо
      зависимости от публичного `sslip.io` alias.

## Release And Closure Evidence

- deployed SHA: отсутствует — `vpn-server` checkout не имеет настроенного
  remote и содержит предшествующие незакоммиченные изменения;
- deploy path: host-local Docker Compose, только service `mtproto`;
- regression checks: Compose validation OK; port `1443` LISTEN; container
  running, OOM false; runtime protocols `[mtp_fake_tls,mtp_secure]`; official
  `mtp_ping` secure/DD path 3/3 DC OK; fresh Telegram DC handshakes;
- post-deploy verification: root filesystem 89%, MTProto log directory 47 MiB,
  current-day log retained; пользователь подтвердил рабочее подключение
  Telegram Desktop 7.0.6 через DD.
- recurrence verification 2026-07-31: container running, OOM false, restart
  count 0, `0.0.0.0:1443` LISTEN, 49 established sessions от Desktop peer,
  fresh Telegram DC handshakes; старый лог 48,703,203 bytes удалён, активный
  current-day log 20,664,346 bytes сохранён; root filesystem 94% (4.6 GiB
  available); пользователь подтвердил восстановление подключения.

## Prevention

Размер логов теперь ограничен конфигурацией при каждом старте контейнера, а
активный лог переживает recreate благодаря host mount. Повторные изменения
MTProto обязаны проверять не только TCP listener, но и Telegram DC handshake.

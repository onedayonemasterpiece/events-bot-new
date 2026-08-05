# Правила розыгрыша — требуется повторное утверждение

> **Статус:** `BLOCKED_OWNER_REBASELINE`.
> Прежняя формула `12 артефактов`, threshold `10 из 12` и связанные шансы больше не являются текущим требованием.

## Почему заблокировано

Текущий candidate использует первую коллекцию из семи реальных артефактов.
Автоматически пересчитать старую формулу нельзя: это изменило бы вероятность
участия, fairness и пользовательские обещания без решения владельца.

До публикации правил нужно отдельно утвердить:

- eligibility и обязательность Auth;
- точный threshold из семи;
- учитываются ли page score/text/screenshot;
- число и источник призов;
- organizer и cutoff;
- immutable snapshot;
- random selection, reserve participant и claim;
- privacy, anti-abuse, accessibility и appeals;
- что происходит с pre-auth local progress после входа.

## Запрещено до утверждения

- показывать старые условия;
- считать localStorage источником eligibility;
- начислять шансы за positive score/NPS;
- обещать конкретный спектакль без подтверждённого prize inventory;
- запускать draw без опубликованных правил и server ledger.

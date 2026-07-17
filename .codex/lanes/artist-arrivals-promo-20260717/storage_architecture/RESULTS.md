# Storage architecture results

- Decision: keep the 1,235-row identity seed as versioned canonical JSON;
  keep mutable locality/appearance/issue/ledger state in Fly SQLite; export a
  sanitized static JSON projection; store media bytes in Object Storage/CDN.
- YDB rejected for this event-domain state because it creates a cross-database
  join, IAM and synchronization boundary without a size, durability or query
  benefit.
- Seed size observed: about 1.49 MiB minified / 76 KiB gzip; identity-only
  projection about 411 KiB / 24 KiB gzip.
- Daily defaults: 14-day horizon, 3 unique artists, preferred 4, 2 unique
  projects, 8 cards. Artist+project is the durable repeat-suppression key;
  completion requires successful TG and VK delivery, so one failed surface is
  not silently lost.
- Reused paths: `Database.init`, SQLModel/Alembic, `ops_run`, APScheduler,
  static-site-build coalescing and existing TG/VK transports.

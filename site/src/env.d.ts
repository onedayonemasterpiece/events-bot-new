/// <reference path="../.astro/types.d.ts" />
/// <reference types="astro/client" />

interface ImportMetaEnv {
  readonly PUBLIC_SERVICE_SHARE_DESKTOP_MODE?: 'd0' | 'd1' | 'd2';
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

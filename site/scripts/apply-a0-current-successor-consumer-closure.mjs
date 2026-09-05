import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { runA0ConsumerClosure } from './a0-current-successor-consumer-closure-lib.mjs';

export {
  A0_CONSUMER_CLOSURE_PATHS,
  EXHIBITIONS_PRIVATE_THEME_ALIASES,
  EXHIBITIONS_REQUIRED_CENTRAL_BINDINGS,
  EXHIBITIONS_REQUIRED_FR0_BINDINGS,
  EXHIBITIONS_RUNTIME_VARIABLES,
  assertA0ConsumerPostconditions,
  runA0ConsumerClosure,
  transformA0Consumer,
} from './a0-current-successor-consumer-closure-lib.mjs';

/*
 * Static markers intentionally live in the canonical entrypoint because the
 * F0 source gate inspects this file before R0 executes it. They define the
 * exact semantic and ownership boundaries; implementation remains in the
 * imported library above.
 */
export const F0_A0_STATIC_INSPECTION_CONTRACT = Object.freeze({
  festival: Object.freeze({
    action_surface: '--ke-color-festival-guide-like-surface',
    taxonomy_surface: '--ke-color-festival-category-surface',
    relation: 'guide-like-action != editorial-category-taxonomy',
  }),
  exhibitions: Object.freeze({
    runtime_layout_variables: Object.freeze([
      '--ex-media-column',
      '--ex-row-gap',
      '--ex-row-radius',
      '--ex-surface-start',
      '--ex-rail-color',
    ]),
    central_icon_roles: Object.freeze([
      '--ke-exhibitions-signal-icon-size',
      '--ke-exhibitions-action-icon-size',
      '--ke-exhibitions-gallery-arrow-icon-size',
    ]),
    canonical_navigation: Object.freeze([
      '<SemanticIcon name="arrow-left" role="control" />',
      '<SemanticIcon name="arrow-right" role="control" />',
    ]),
  }),
});

const invokedAsScript = process.argv[1]
  && pathToFileURL(resolve(process.argv[1])).href === import.meta.url;

if (invokedAsScript) {
  runA0ConsumerClosure({ checkOnly: process.argv.includes('--check') }).catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

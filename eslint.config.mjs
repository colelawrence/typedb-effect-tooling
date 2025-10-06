//  @ts-check
import eslint from "@eslint/js";
import simpleImportSort from "eslint-plugin-simple-import-sort";
import { defineConfig } from "eslint/config";
import tseslint from "typescript-eslint";

const ignores = ["**/dist/*", "assets", "**/.venv"];

export default defineConfig([
  {
    ignores,
  },
  // Import sorting
  {
    files: ["**/*.{ts,tsx,js,jsx}"],
    ignores: ["frontend/**/components/ui/**"],
    plugins: {
      "simple-import-sort": simpleImportSort,
      // import: importPlugin,
    },
    rules: {
      "simple-import-sort/imports": [
        "error",
        {
          groups: [
            // Side effect imports.
            ["^\\u0000"],
            // Node.js builtins prefixed with `node:`.
            ["^node:"],
            // Packages.
            // Things that start with a letter (or digit or underscore), or `@` followed by a letter.
            ["^@?\\w"],
            // Absolute imports and other imports such as Vue-style `@/foo`.
            // Anything not matched in another group.
            ["^"],
            // Group @saga packages separately
            ["^@saga.*"],
            // Relative imports.
            // Anything that starts with a dot.
            ["^\\."],
          ],
        },
      ],
      "simple-import-sort/exports": "error",
    },
  },
  eslint.configs.recommended,
  tseslint.configs.recommended,
  {
    rules: {
      "@typescript-eslint/no-unused-vars": "warn",
      "@typescript-eslint/no-empty-object-type": "off",
      "no-empty-pattern": "off",
      "require-yield": "off",
    },
  },
  {
    files: ["backend/test/**/*.ts"],
    ignores: ["backend/test/setup.ts"],
    rules: {
      "no-restricted-imports": [
        "error",
        {
          paths: [
            {
              name: "@saga/database/client",
              importNames: ["Db"],
              message: "Don't use `Db` directly. Use `TestDb` instead.",
            },
          ],
        },
      ],
    },
  },
]);

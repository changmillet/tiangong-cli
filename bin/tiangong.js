#!/usr/bin/env node

import { existsSync } from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath, pathToFileURL } from 'node:url';

const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const entryPath = path.join(rootDir, 'dist', 'src', 'main.js');

export async function runFromBin(argv = process.argv.slice(2), env = process.env) {
  if (!existsSync(entryPath)) {
    throw new Error(
      "Missing built CLI artifacts at 'dist/src/main.js'. Run 'npm run build' or reinstall dependencies to regenerate dist.",
    );
  }

  const entryUrl = pathToFileURL(entryPath).href;
  const { main } = await import(entryUrl);
  return main(argv, env);
}

const invokedUrl = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href : null;

if (invokedUrl && import.meta.url === invokedUrl) {
  try {
    process.exitCode = await runFromBin();
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exitCode = 1;
  }
}

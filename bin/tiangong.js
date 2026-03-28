#!/usr/bin/env node

import path from 'node:path';
import process from 'node:process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { register } from 'tsx/esm/api';

const rootDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

export async function runFromBin(argv = process.argv.slice(2), env = process.env) {
  const unregister = register();
  try {
    const entryUrl = pathToFileURL(path.join(rootDir, 'src', 'main.ts')).href;
    const { main } = await import(entryUrl);
    return main(argv, env);
  } finally {
    unregister();
  }
}

const invokedUrl = process.argv[1] ? pathToFileURL(path.resolve(process.argv[1])).href : null;

if (invokedUrl && import.meta.url === invokedUrl) {
  process.exitCode = await runFromBin();
}

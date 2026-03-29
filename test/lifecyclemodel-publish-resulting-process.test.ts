import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  runLifecyclemodelPublishResultingProcess,
  type LifecyclemodelPublishResultingProcessReport,
} from '../src/lib/lifecyclemodel-publish-resulting-process.js';
import { CliError } from '../src/lib/errors.js';

function writeJson(filePath: string, value: unknown): void {
  writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function readJson<T>(filePath: string): T {
  return JSON.parse(readFileSync(filePath, 'utf8')) as T;
}

function assertCliErrorCode(expectedCode: string) {
  return (error: unknown): boolean => {
    assert.ok(error instanceof CliError);
    assert.equal(error.code, expectedCode);
    return true;
  };
}

test('runLifecyclemodelPublishResultingProcess writes publish bundle artifacts for selected payloads', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-lm-publish-full-'));

  writeJson(path.join(dir, 'process-projection-bundle.json'), {
    source_model: {
      id: 'lm-demo',
      version: '00.00.001',
    },
    projected_processes: [{ id: 'proc-1' }, { id: 'proc-2' }],
    relations: [{ id: 'rel-1' }],
  });
  writeJson(path.join(dir, 'projection-report.json'), {
    status: 'prepared_local_bundle',
  });

  try {
    const report = await runLifecyclemodelPublishResultingProcess({
      runDir: dir,
      publishProcesses: true,
      publishRelations: true,
      now: new Date('2026-03-29T00:00:00.000Z'),
    });

    assert.deepEqual(report, {
      generated_at_utc: '2026-03-29T00:00:00.000Z',
      run_dir: dir,
      status: 'prepared_local_publish_bundle',
      publish_processes: true,
      publish_relations: true,
      counts: {
        projected_processes: 2,
        relations: 1,
      },
      source_model: {
        id: 'lm-demo',
        version: '00.00.001',
      },
      files: {
        projection_bundle: path.join(dir, 'process-projection-bundle.json'),
        projection_report: path.join(dir, 'projection-report.json'),
        publish_bundle: path.join(dir, 'publish-bundle.json'),
        publish_intent: path.join(dir, 'publish-intent.json'),
      },
    } satisfies LifecyclemodelPublishResultingProcessReport);

    assert.deepEqual(readJson(path.join(dir, 'publish-bundle.json')), {
      generated_at: '2026-03-29T00:00:00.000Z',
      run_dir: dir,
      source_model: {
        id: 'lm-demo',
        version: '00.00.001',
      },
      publish_processes: true,
      publish_relations: true,
      status: 'prepared_local_publish_bundle',
      projected_processes: [{ id: 'proc-1' }, { id: 'proc-2' }],
      relations: [{ id: 'rel-1' }],
      report: {
        status: 'prepared_local_bundle',
      },
    });
    assert.deepEqual(readJson(path.join(dir, 'publish-intent.json')), {
      ok: true,
      command: 'publish',
      run_dir: dir,
      publish_processes: true,
      publish_relations: true,
      status: 'prepared_local_publish_bundle',
    });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runLifecyclemodelPublishResultingProcess can independently disable projected process publishing', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-lm-publish-relations-only-'));

  writeJson(path.join(dir, 'process-projection-bundle.json'), {
    source_model: {
      id: 'lm-demo',
    },
    projected_processes: [{ id: 'proc-1' }],
    relations: [{ id: 'rel-1' }],
  });
  writeJson(path.join(dir, 'projection-report.json'), {
    status: 'prepared_local_bundle',
  });

  try {
    const report = await runLifecyclemodelPublishResultingProcess({
      runDir: dir,
      publishProcesses: false,
      publishRelations: true,
    });

    assert.deepEqual(report.counts, {
      projected_processes: 0,
      relations: 1,
    });

    assert.deepEqual(readJson(path.join(dir, 'publish-bundle.json')), {
      generated_at: report.generated_at_utc,
      run_dir: dir,
      source_model: {
        id: 'lm-demo',
      },
      publish_processes: false,
      publish_relations: true,
      status: 'prepared_local_publish_bundle',
      projected_processes: [],
      relations: [{ id: 'rel-1' }],
      report: {
        status: 'prepared_local_bundle',
      },
    });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runLifecyclemodelPublishResultingProcess can independently disable relation publishing', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-lm-publish-processes-only-'));

  writeJson(path.join(dir, 'process-projection-bundle.json'), {
    source_model: {
      id: 'lm-demo',
    },
    projected_processes: [{ id: 'proc-1' }],
    relations: [{ id: 'rel-1' }],
  });
  writeJson(path.join(dir, 'projection-report.json'), {
    status: 'prepared_local_bundle',
  });

  try {
    const report = await runLifecyclemodelPublishResultingProcess({
      runDir: dir,
      publishProcesses: true,
      publishRelations: false,
      now: new Date('2026-03-29T00:00:01.000Z'),
    });

    assert.deepEqual(report.counts, {
      projected_processes: 1,
      relations: 0,
    });

    assert.deepEqual(readJson(path.join(dir, 'publish-bundle.json')), {
      generated_at: '2026-03-29T00:00:01.000Z',
      run_dir: dir,
      source_model: {
        id: 'lm-demo',
      },
      publish_processes: true,
      publish_relations: false,
      status: 'prepared_local_publish_bundle',
      projected_processes: [{ id: 'proc-1' }],
      relations: [],
      report: {
        status: 'prepared_local_bundle',
      },
    });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runLifecyclemodelPublishResultingProcess handles scalar and null projection entries', async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-lm-publish-scalar-'));

  writeJson(path.join(dir, 'process-projection-bundle.json'), {
    source_model: 'not-an-object',
    projected_processes: { id: 'proc-scalar' },
    relations: null,
  });
  writeJson(path.join(dir, 'projection-report.json'), {
    status: 'prepared_local_bundle',
  });

  try {
    const report = await runLifecyclemodelPublishResultingProcess({
      runDir: dir,
      publishProcesses: true,
      publishRelations: true,
    });

    assert.deepEqual(report.source_model, {});
    assert.deepEqual(report.counts, {
      projected_processes: 1,
      relations: 0,
    });

    assert.deepEqual(readJson(path.join(dir, 'publish-bundle.json')), {
      generated_at: report.generated_at_utc,
      run_dir: dir,
      source_model: {},
      publish_processes: true,
      publish_relations: true,
      status: 'prepared_local_publish_bundle',
      projected_processes: [{ id: 'proc-scalar' }],
      relations: [],
      report: {
        status: 'prepared_local_bundle',
      },
    });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('runLifecyclemodelPublishResultingProcess validates required run-dir and artifacts', async () => {
  await assert.rejects(
    () =>
      runLifecyclemodelPublishResultingProcess({
        runDir: '',
        publishProcesses: true,
        publishRelations: true,
      }),
    assertCliErrorCode('LIFECYCLEMODEL_RUN_DIR_REQUIRED'),
  );

  const missingBundleDir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-lm-publish-missing-bundle-'));
  const invalidBundleDir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-lm-publish-invalid-bundle-'));
  const invalidReportDir = mkdtempSync(path.join(os.tmpdir(), 'tg-cli-lm-publish-invalid-report-'));

  writeJson(path.join(missingBundleDir, 'projection-report.json'), {
    status: 'prepared_local_bundle',
  });
  writeJson(path.join(invalidBundleDir, 'process-projection-bundle.json'), []);
  writeJson(path.join(invalidBundleDir, 'projection-report.json'), {
    status: 'prepared_local_bundle',
  });
  writeJson(path.join(invalidReportDir, 'process-projection-bundle.json'), {
    projected_processes: [],
  });
  writeJson(path.join(invalidReportDir, 'projection-report.json'), []);

  try {
    await assert.rejects(
      () =>
        runLifecyclemodelPublishResultingProcess({
          runDir: missingBundleDir,
          publishProcesses: true,
          publishRelations: true,
        }),
      assertCliErrorCode('LIFECYCLEMODEL_PROJECTION_BUNDLE_MISSING'),
    );

    await assert.rejects(
      () =>
        runLifecyclemodelPublishResultingProcess({
          runDir: invalidBundleDir,
          publishProcesses: true,
          publishRelations: true,
        }),
      assertCliErrorCode('LIFECYCLEMODEL_PROJECTION_BUNDLE_NOT_OBJECT'),
    );

    await assert.rejects(
      () =>
        runLifecyclemodelPublishResultingProcess({
          runDir: invalidReportDir,
          publishProcesses: true,
          publishRelations: true,
        }),
      assertCliErrorCode('LIFECYCLEMODEL_PROJECTION_REPORT_NOT_OBJECT'),
    );
  } finally {
    rmSync(missingBundleDir, { recursive: true, force: true });
    rmSync(invalidBundleDir, { recursive: true, force: true });
    rmSync(invalidReportDir, { recursive: true, force: true });
  }
});

#!/usr/bin/env node
/*
 * Regression harness for the scope predicate used by .github/workflows/docs-check.yml.
 *
 * Four cases, each of which was a real risk at some point:
 *   no-docs        - a PR that touches neither docs nor this workflow must not run the toolchain
 *   workflow-only  - a PR that only edits docs-check.yml must run the check it modifies
 *   docs-only      - the normal case
 *   large-list     - docs first, then thousands of other paths: the shell version of this predicate
 *                    died on SIGPIPE here and reported false (review 2026-11-18, comment 6978765146)
 *
 * The cases drive `check-docs-release.js --scope --paths-file`, i.e. exactly what the workflow runs,
 * so the predicate under test is the one in production, not a copy.
 */
const { execFileSync } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const CHECK = path.join(__dirname, 'check-docs-release.js');

function runScope(paths, githubOutput) {
  const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'scope-')), 'paths.txt');
  fs.writeFileSync(file, paths.join('\n') + '\n');
  const env = { ...process.env, GITHUB_OUTPUT: githubOutput || '' };
  const out = execFileSync(process.execPath, [CHECK, '--scope', '--paths-file', file], { encoding: 'utf8', env });
  return out;
}

function docsChangedOf(output, outputFile) {
  if (outputFile) {
    const written = fs.readFileSync(outputFile, 'utf8').trim().split('\n').filter(Boolean);
    const line = written.find((l) => l.startsWith('docs_changed='));
    if (!line) throw new Error('no docs_changed written to GITHUB_OUTPUT');
    return line.split('=')[1];
  }
  const m = output.match(/docs_changed=(true|false)/);
  if (!m) throw new Error('no docs_changed in stdout');
  return m[1];
}

const noise = (n) => Array.from({ length: n }, (_, i) => `src/main/java/com/aliyun/odps/p${i}/Chunk${i}.java`);
const cases = [
  ['no-docs', ['README.md', 'odps-sdk/odps-sdk-core/src/main/java/com/aliyun/odps/Odps.java', '.github/workflows/main.yml'], 'false'],
  ['workflow-only', ['.github/workflows/docs-check.yml'], 'true'],
  ['docs-only', ['docs/docs/changelog.md', 'docs/static/llms-full.txt'], 'true'],
  ['large-list', ['docs/index.md', ...noise(10000)], 'true'],
];

/*
 * Shape assertions. actionlint validates syntax only: deleting the regressions step, or turning the
 * gate back into a branch that reports success when the scope is unknown, keeps actionlint quiet at
 * rc=0 (measured). So the semantic contract of the workflow is asserted here, against the real file.
 * Override the path with DOCS_CHECK_WORKFLOW to point this at a copy under review.
 */
const WORKFLOW = process.env.DOCS_CHECK_WORKFLOW
  || path.join(__dirname, '..', '..', '.github', 'workflows', 'docs-check.yml');

/*
 * Per-step check: an earlier version of this assertion counted guard lines (>= 3), which still passed
 * when one of the three steps lost its guard. Splitting the job into steps and inspecting each toolchain
 * step is the version that actually catches it.
 */
function unguardedToolchainSteps(wf) {
  // 只看 docs-check 这个 job：maven-drift 的 setup-node 本来就不受 scope 控制（它每次都要跑 --drift），
  // 把它算进来会得到假阳性——本轮第一版就是这么"红"的。
  const from = wf.indexOf('  docs-check:');
  const to = wf.indexOf('\n  maven-drift:');
  const job = from < 0 ? '' : wf.slice(from, to < 0 ? wf.length : to);
  const stepGuard = /if: steps\.scope\.outputs\.docs_changed == 'true'/;
  const bad = [];
  for (const step of job.split(/\n(?= {6}- )/)) {
    const isToolchain = /(^|\n) {8}run: (npm ci|node scripts\/check-docs-release\.js --build)/.test(step)
      || step.includes('uses: actions/setup-node@v4');
    if (isToolchain && !stepGuard.test(step)) bad.push(step.split('\n')[0].trim().replace(/^- /, ''));
  }
  return bad;
}

function codeOnly(text) {
  return text.split('\n').filter((l) => {
    const t = l.trim();
    return t && !t.startsWith('*') && !t.startsWith('/*') && !t.startsWith('//') && !t.startsWith('#');
  }).join('\n');
}

function shapeChecks(wf, scopeSource) {
  const gateIdx = wf.indexOf('cannot tell whether this run was in docs scope');
  const guarded = (wf.match(/if: steps\.scope\.outputs\.docs_changed == 'true'/g) || []).length;
  return [
    ['regressions run in CI', wf.includes('node scripts/docs-scope-regressions.js')],
    ['scope decided by the shared predicate', wf.includes('check-docs-release.js --scope')],
    ['every toolchain step is scope-guarded', `unguarded=${JSON.stringify(unguardedToolchainSteps(wf))}`, unguardedToolchainSteps(wf).length === 0],
    ['gate refuses an unknown scope', gateIdx >= 0 && wf.slice(gateIdx, gateIdx + 200).includes('exit 1')],
    ['gate fails on non-success job status', wf.includes('if [ "${{ job.status }}" != "success" ]; then')],
    ['push trigger keeps its path filter', wf.includes('- "release/**"')],
    ['workflow file itself is in scope', scopeSource.includes("SCOPE_EXACT = ['.github/workflows/docs-check.yml']")],
    // 只看代码行：解释 SIGPIPE 背景的注释里本来就写着 grep -q，把注释也算进去会变假警报（本轮就是这么触发的）
    ['no pipe into grep left in the predicate', !codeOnly(scopeSource + '\n' + wf).includes('grep -q')],
  ];
}

let failed = 0;
for (const [name, paths, expect] of cases) {
  const outDir = fs.mkdtempSync(path.join(os.tmpdir(), 'scope-out-'));
  const outputFile = path.join(outDir, 'github_output.txt');
  let got = 'error';
  try {
    got = docsChangedOf(runScope(paths, outputFile), outputFile);
  } catch (e) {
    console.log(`FAIL ${name}: ${e.message}`);
    failed++;
    continue;
  }
  const written = fs.readFileSync(outputFile, 'utf8').trim();
  if (got !== expect) { console.log(`FAIL ${name}: docs_changed=${got}, expected ${expect}`); failed++; continue; }
  if (written !== `docs_changed=${expect}`) { console.log(`FAIL ${name}: GITHUB_OUTPUT payload "${written}"`); failed++; continue; }
  console.log(`PASS ${name}: docs_changed=${got} payload=${written} inputs=${paths.length}`);
}
console.log(`shape file under test: ${WORKFLOW}`);
const wfText = fs.existsSync(WORKFLOW) ? fs.readFileSync(WORKFLOW, 'utf8') : '';
const scopeText = fs.existsSync(CHECK) ? fs.readFileSync(CHECK, 'utf8') : '';
if (!wfText) {
  console.log(`FAIL shape: workflow file not readable (${WORKFLOW})`);
  failed++;
} else {
  for (const entry of shapeChecks(wfText, scopeText)) {
    const [name, detail, pass] = entry.length === 3 ? entry : [entry[0], '', entry[1]];
    if (pass) { console.log(`PASS shape ${name}${detail ? ` (${detail})` : ''}`); continue; }
    console.log(`FAIL shape ${name}${detail ? ` (${detail})` : ''}`);
    failed++;
  }
}

console.log(`${failed === 0 ? 'SCOPE REGRESSIONS PASSED' : 'SCOPE REGRESSIONS FAILED'} cases=${cases.length} failed=${failed}`);
process.exit(failed === 0 ? 0 : 1);

#!/usr/bin/env node
/*
 * Reproducible docs check for the SDK documentation site.
 *
 * Modes
 *   node scripts/check-docs-release.js           PR check against the pinned release in scripts/expected-release.json
 *   node scripts/check-docs-release.js --build    additionally run `npm run build` and verify the generated site
 *   node scripts/check-docs-release.js --drift     compare the pin with the live Maven Central <release> (scheduled job)
 *
 * What the PR check guarantees, in order:
 *   1. the install snippets and the changelog head in docs/docs use the pinned release;
 *   2. the checked-in llms.txt / llms-full.txt are exactly what scripts/generate-llms-txt.js produces from those docs;
 *   3. inside the generated full text, the installation section carries the pinned release and the changelog
 *      section starts with it;
 *   4. with --build, the built HTML pages and the built llms-full.txt carry it too.
 *
 * Everything is derived from files in the repository, so the check is replayable offline except in --drift mode.
 */
const { execFileSync, spawnSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const https = require('https');

const DOCS_DIR = path.resolve(__dirname, '..');
const REPO_DIR = path.resolve(DOCS_DIR, '..');
const PIN_FILE = path.join(DOCS_DIR, 'scripts', 'expected-release.json');
const CENTRAL_URL = 'https://repo1.maven.org/maven2/com/aliyun/odps/odps-sdk-core/maven-metadata.xml';
const VERSION_RE = /\b\d+\.\d+\.\d+-public\b/g;

const mode = process.argv.slice(2).find((a) => a.startsWith('--')) || '';
const failures = [];
const notes = [];

function fail(step, detail) { failures.push(`${step}: ${detail}`); console.error(`FAIL ${step}: ${detail}`); }
function ok(step, detail) { notes.push(`${step}: ${detail}`); console.log(`PASS ${step}: ${detail}`); }

function readPin() {
  if (!fs.existsSync(PIN_FILE)) { fail('pin', `${path.relative(REPO_DIR, PIN_FILE)} is missing`); return null; }
  let pin;
  try {
    pin = JSON.parse(fs.readFileSync(PIN_FILE, 'utf8')).expectedRelease;
  } catch (e) { fail('pin', `cannot parse ${path.relative(REPO_DIR, PIN_FILE)}: ${e.message}`); return null; }
  if (!pin || !/^\d+\.\d+\.\d+-public$/.test(pin)) { fail('pin', `expectedRelease is not an x.y.z-public value: ${pin}`); return null; }
  return pin;
}

function versionsIn(text) {
  const found = text.match(VERSION_RE) || [];
  return { total: found.length, distinct: Array.from(new Set(found)).sort() };
}

function checkSourceDocs(pin) {
  for (const rel of ['docs/getting-started/installation.md', 'docs/index.md']) {
    const file = path.join(DOCS_DIR, rel);
    if (!fs.existsSync(file)) { fail('source-snippets', `${rel} is missing`); continue; }
    const { total, distinct } = versionsIn(fs.readFileSync(file, 'utf8'));
    if (total === 0) fail('source-snippets', `${rel} mentions no x.y.z-public release`);
    else if (distinct.length !== 1 || distinct[0] !== pin) fail('source-snippets', `${rel} uses ${distinct.join(', ')}; expected only ${pin}`);
    else ok('source-snippets', `${rel}: ${total} snippet occurrences, all ${pin}`);
  }
  const changelog = path.join(DOCS_DIR, 'docs', 'changelog.md');
  if (!fs.existsSync(changelog)) fail('source-changelog', 'docs/changelog.md is missing');
  else {
    const head = (fs.readFileSync(changelog, 'utf8').match(/^## \[(\d+\.\d+\.\d+-public)\]/m) || [])[1];
    if (head !== pin) fail('source-changelog', `newest changelog entry is ${head || 'none'}; expected ${pin}`);
    else ok('source-changelog', `newest entry is ${pin}`);
  }
}

function regenerate() {
  try {
    const out = execFileSync('node', [path.join(DOCS_DIR, 'scripts', 'generate-llms-txt.js')], { cwd: DOCS_DIR, encoding: 'utf8' });
    return out.split('\n').filter((l) => l.startsWith('Generated')).join(' | ');
  } catch (e) { fail('regenerate', `scripts/generate-llms-txt.js exited non-zero: ${e.message}`); return null; }
}

function checkGeneratedIsCommitted() {
  const before = execFileSync('git', ['-C', REPO_DIR, 'status', '--porcelain', '--', 'docs/static/llms.txt', 'docs/static/llms-full.txt'], { encoding: 'utf8' }).trim();
  if (before !== '') { fail('generated-in-sync', 'before regeneration these files were already modified, commit the regenerated copies first'); return; }
  const summary = regenerate();
  if (summary) console.log(`     ${summary}`);
  const after = execFileSync('git', ['-C', REPO_DIR, 'status', '--porcelain', '--', 'docs/static/llms.txt', 'docs/static/llms-full.txt'], { encoding: 'utf8' }).trim();
  if (after === '') ok('generated-in-sync', 'checked-in llms.txt and llms-full.txt match a fresh run of the generator');
  else fail('generated-in-sync', `checked-in llms copies are stale; run \`node scripts/generate-llms-txt.js\` in docs/ and commit (${after.replace(/\n/g, ' ')})`);
}

function fullTextSections() {
  const text = fs.readFileSync(path.join(DOCS_DIR, 'static', 'llms-full.txt'), 'utf8');
  const sections = new Map();
  let current = null;
  for (const line of text.split('\n')) {
    if (line === '---') { current = null; continue; }
    if (current === null && line.startsWith('# ')) { current = line.slice(2).trim(); sections.set(current, []); continue; }
    if (current !== null) sections.get(current).push(line);
  }
  return sections;
}

function checkFullText(pin) {
  let sections;
  try { sections = fullTextSections(); } catch (e) { fail('full-text', `cannot read static/llms-full.txt: ${e.message}`); return; }
  const inst = sections.get('getting-started/installation.md');
  if (!inst) fail('full-text', 'installation section is missing from llms-full.txt');
  else {
    const { total, distinct } = versionsIn(inst.join('\n'));
    if (total === 0 || distinct.length !== 1 || distinct[0] !== pin) fail('full-text', `installation section uses ${distinct.join(', ') || 'nothing'}; expected only ${pin}`);
    else ok('full-text', `installation section: ${total} occurrences of ${pin}`);
  }
  const ch = sections.get('changelog.md');
  if (!ch) fail('full-text', 'changelog section is missing from llms-full.txt');
  else {
    const head = (ch.join('\n').match(/^## \[(\d+\.\d+\.\d+-public)\]/m) || [])[1];
    if (head !== pin) fail('full-text', `changelog section starts at ${head || 'none'}; expected ${pin}`);
    else ok('full-text', `changelog section starts at ${pin}`);
  }
}

function buildAndVerify(pin) {
  const res = spawnSync('npm', ['run', 'build'], { cwd: DOCS_DIR, encoding: 'utf8', shell: false });
  if (res.status !== 0) {
    fail('build', `npm run build exited ${res.status}: ${(res.stderr || res.stdout || '').split('\n').filter(Boolean).slice(-3).join(' / ')}`);
    return;
  }
  ok('build', 'npm run build exited 0');
  const checks = [
    ['build/changelog/index.html', `contains ${pin}`, (t) => t.includes(pin)],
    ['build/getting-started/installation/index.html', `contains ${pin}`, (t) => t.includes(pin)],
    ['build/index.html', `contains ${pin}`, (t) => t.includes(pin)],
    ['build/llms-full.txt', `starts its changelog at ${pin}`, (t) => ((t.match(/^## \[(\d+\.\d+\.\d+-public)\]/m) || [])[1] === pin)],
  ];
  for (const [rel, label, test] of checks) {
    const file = path.join(DOCS_DIR, rel);
    if (!fs.existsSync(file)) { fail('build-output', `${rel} is missing`); continue; }
    const text = fs.readFileSync(file, 'utf8');
    const hits = (text.match(VERSION_RE) || []).filter((v) => v === pin).length;
    if (!test(text)) fail('build-output', `${rel} failed the assertion (${label}, pinned hits=${hits})`);
    else ok('build-output', `${rel}: ${label}, pinned hits=${hits}`);
  }
  const inst = fs.readFileSync(path.join(DOCS_DIR, 'build', 'getting-started', 'installation', 'index.html'), 'utf8');
  const stale = (inst.match(VERSION_RE) || []).filter((v) => v !== pin);
  if (stale.length) fail('build-output', `built installation page still references ${Array.from(new Set(stale)).join(', ')}`);
  else ok('build-output', 'built installation page has no other -public release');
}

function checkDrift(pin) {
  return new Promise((resolve) => {
    if (!pin) { resolve(); return; }
    https.get(CENTRAL_URL, { timeout: 20000 }, (res) => {
      let body = '';
      res.on('data', (c) => (body += c));
      res.on('end', () => {
        const rel = (body.match(/<release>([^<]+)<\/release>/) || [])[1];
        if (res.statusCode !== 200 || !rel) fail('maven-drift', `HTTP ${res.statusCode} and no <release> parsed from the metadata`);
        else if (rel !== pin) fail('maven-drift', `Maven Central publishes ${rel} but docs pin ${pin}: bump docs (snippets, changelog, scripts/expected-release.json) and regenerate the llms copies`);
        else ok('maven-drift', `Maven Central <release> ${rel} equals the docs pin`);
        resolve();
      });
    }).on('error', (e) => { fail('maven-drift', `could not reach Maven Central: ${e.message}`); resolve(); });
  });
}

async function main() {
  const pin = readPin();
  if (mode === '--drift') {
    await checkDrift(pin);
  } else {
    if (!pin) process.exit(1);
    checkSourceDocs(pin);
    checkGeneratedIsCommitted();
    checkFullText(pin);
    if (mode === '--build') buildAndVerify(pin);
  }
  console.log(`\n${failures.length === 0 ? 'DOCS CHECK PASSED' : 'DOCS CHECK FAILED'} (pin=${pin}, mode=${mode || 'pr-check'}, steps passed=${notes.length}, failed=${failures.length})`);
  process.exit(failures.length === 0 ? 0 : 1);
}

main();

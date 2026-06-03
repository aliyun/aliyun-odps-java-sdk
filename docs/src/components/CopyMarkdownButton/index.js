import React, {useState, useCallback} from 'react';
import {useDoc} from '@docusaurus/plugin-content-docs/client';

export default function CopyMarkdownButton() {
  const [copied, setCopied] = useState(false);
  const {metadata, frontMatter} = useDoc();

  const handleCopy = useCallback(async () => {
    const sourcePath = metadata.source?.replace('@site/docs/', '') || metadata.editUrl;
    if (!sourcePath) return;

    try {
      const cleanPath = sourcePath.replace(/^@site\/docs\//, '');
      const resp = await fetch(
        `https://raw.githubusercontent.com/aliyun/aliyun-odps-java-sdk/release/0.57.x/docs/docs/${cleanPath}`
      );
      if (!resp.ok) throw new Error('fetch failed');
      const text = await resp.text();
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      const pageText = document.querySelector('.markdown')?.innerText;
      if (pageText) {
        await navigator.clipboard.writeText(pageText);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
      }
    }
  }, [metadata]);

  return (
    <div style={{display: 'flex', justifyContent: 'flex-end', marginBottom: '-0.5rem'}}>
      <button
        className={`copy-markdown-btn${copied ? ' copied' : ''}`}
        onClick={handleCopy}
        title="复制为 Markdown">
        {copied ? '✓ 已复制' : '⎘ 复制 Markdown'}
      </button>
    </div>
  );
}

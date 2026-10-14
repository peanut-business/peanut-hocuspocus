import * as Y from 'yjs';
import { Schema, type NodeSpec, type MarkSpec } from '@tiptap/pm/model';
import {
  prosemirrorJSONToYDoc,
  yXmlFragmentToProsemirrorJSON,
} from '@tiptap/y-tiptap';
import { phpJson, sha } from './digests.js';
export const projectorVersion = 'peanut.richtext-yjs/1';
export const fragmentName = 'default';
export interface Structured {
  schemaVersion: 'peanut.richtext/1';
  editorModel: 'tiptap-prosemirror';
  content: Record<string, unknown>;
  annotations: [];
}
const attrs = (keys: string[]): NonNullable<NodeSpec['attrs']> =>
  Object.fromEntries(keys.map((k) => [k, { default: null }]));
// Structural PM schema matches the existing RichText node/mark vocabulary; no HTML renderer.
const nodes: Record<string, NodeSpec> = {
  doc: { content: 'block+' },
  text: { group: 'inline' },
  paragraph: {
    group: 'block',
    content: 'inline*',
    attrs: attrs(['textAlign']),
  },
  heading: {
    group: 'block',
    content: 'inline*',
    attrs: { level: { default: 1 }, textAlign: { default: null } },
  },
  blockquote: { group: 'block', content: 'block+' },
  bulletList: { group: 'block', content: 'listItem+' },
  orderedList: {
    group: 'block',
    content: 'listItem+',
    attrs: { start: { default: 1 } },
  },
  listItem: { content: 'paragraph block*' },
  codeBlock: {
    group: 'block',
    content: 'text*',
    marks: '',
    attrs: attrs(['language']),
  },
  hardBreak: { group: 'inline', inline: true },
  horizontalRule: { group: 'block' },
  table: { group: 'block', content: 'tableRow+' },
  tableRow: { content: '(tableCell | tableHeader)+' },
  tableCell: {
    content: 'block+',
    attrs: {
      colspan: { default: 1 },
      rowspan: { default: 1 },
      colwidth: { default: null },
    },
  },
  tableHeader: {
    content: 'block+',
    attrs: {
      colspan: { default: 1 },
      rowspan: { default: 1 },
      colwidth: { default: null },
    },
  },
};
for (const name of ['image', 'video', 'audio', 'file', 'media'])
  nodes[name] = {
    group: 'block',
    atom: true,
    attrs: {
      ...attrs(['fileKey', 'variantKey', 'alt', 'title']),
      kind: { default: 'file' },
    },
  };
const marks: Record<string, MarkSpec> = Object.fromEntries(
  ['bold', 'italic', 'strike', 'underline', 'code'].map((name) => [name, {}])
);
marks.link = {
  attrs: {
    href: { default: null },
    target: { default: '_blank' },
    rel: { default: 'noopener noreferrer' },
    class: { default: null },
  },
};
marks.textStyle = { attrs: attrs(['color']) };
export const schema = new Schema({ nodes, marks });
function validLink(value: unknown): boolean {
  if (
    typeof value !== 'string' ||
    Buffer.byteLength(value) > 2048 ||
    [...value].some((character) => character.charCodeAt(0) <= 32) ||
    value.includes('/storage/delivery')
  )
    return false;
  try {
    const url = new URL(value);
    const authority = /^[a-z][a-z0-9+.-]*:\/\/([^/?#]+)/i.exec(value);
    return (
      ['http:', 'https:', 'mailto:'].includes(url.protocol) &&
      (!['http:', 'https:'].includes(url.protocol) ||
        (!!authority?.[1] && !authority[1].includes('\\') && !!url.hostname))
    );
  } catch {
    return false;
  }
}
function validateAttributes(node: Record<string, unknown>): void {
  if (
    Object.keys(node).some(
      (key) => !['type', 'content', 'text', 'attrs', 'marks'].includes(key)
    )
  )
    throw new Error('PROJECTION_NODE_INVALID');
  const type = String(node.type);
  const specification = nodes[type];
  if (!specification) throw new Error('PROJECTION_NODE_INVALID');
  const attributes = (node.attrs ?? {}) as Record<string, unknown>;
  if (
    !attributes ||
    typeof attributes !== 'object' ||
    Array.isArray(attributes) ||
    Object.keys(attributes).some(
      (key) => !Object.hasOwn(specification.attrs ?? {}, key)
    )
  )
    throw new Error('PROJECTION_ATTRIBUTES_INVALID');
  for (const [key, value] of Object.entries(attributes)) {
    let valid = false;
    switch (key) {
      case 'fileKey':
        valid = typeof value === 'string' && /^file_[0-9a-f]{32}$/.test(value);
        break;
      case 'kind':
        valid =
          typeof value === 'string' &&
          ['image', 'video', 'audio', 'file'].includes(value);
        break;
      case 'variantKey':
        valid =
          value === null ||
          (typeof value === 'string' && /^[a-z][a-z0-9-]{0,31}$/.test(value));
        break;
      case 'alt':
      case 'title':
        valid =
          value === null ||
          (typeof value === 'string' && Buffer.byteLength(value) <= 1000);
        break;
      case 'level':
        valid =
          Number.isInteger(value) && Number(value) >= 1 && Number(value) <= 6;
        break;
      case 'textAlign':
        valid =
          value === null ||
          (typeof value === 'string' &&
            ['left', 'right', 'center', 'justify'].includes(value));
        break;
      case 'start':
        valid =
          Number.isInteger(value) &&
          Number(value) >= 1 &&
          Number(value) <= 1000000;
        break;
      case 'language':
        valid =
          value === null ||
          (typeof value === 'string' && /^[a-zA-Z0-9_+.-]{0,64}$/.test(value));
        break;
      case 'colspan':
      case 'rowspan':
        valid =
          Number.isInteger(value) && Number(value) >= 1 && Number(value) <= 100;
        break;
      case 'colwidth':
        valid =
          value === null ||
          (Array.isArray(value) &&
            value.length <= 100 &&
            value.every(
              (width) => Number.isInteger(width) && width >= 1 && width <= 10000
            ));
        break;
      default:
        valid = false;
    }
    if (!valid) throw new Error('PROJECTION_ATTRIBUTES_INVALID');
  }
  const nodeMarks = node.marks ?? [];
  if (
    !Array.isArray(nodeMarks) ||
    (!['text', 'hardBreak'].includes(type) && nodeMarks.length)
  )
    throw new Error('PROJECTION_MARK_INVALID');
  for (const entry of nodeMarks) {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry))
      throw new Error('PROJECTION_MARK_INVALID');
    const mark = entry as Record<string, unknown>;
    if (
      Object.keys(mark).some((key) => !['type', 'attrs'].includes(key)) ||
      typeof mark.type !== 'string' ||
      !marks[mark.type]
    )
      throw new Error('PROJECTION_MARK_INVALID');
    const markType = mark.type;
    const attributes = (mark.attrs ?? {}) as Record<string, unknown>;
    if (
      !attributes ||
      typeof attributes !== 'object' ||
      Array.isArray(attributes) ||
      Object.keys(attributes).some(
        (key) => !Object.hasOwn(marks[markType]?.attrs ?? {}, key)
      )
    )
      throw new Error('PROJECTION_MARK_INVALID');
    if (
      mark.type === 'link' &&
      (!validLink(attributes.href) ||
        (attributes.target != null &&
          !['_blank', '_self'].includes(String(attributes.target))) ||
        (attributes.rel != null &&
          !['noopener noreferrer', 'noopener noreferrer nofollow'].includes(
            String(attributes.rel)
          )) ||
        attributes.class != null)
    )
      throw new Error('PROJECTION_LINK_INVALID');
    if (
      mark.type === 'textStyle' &&
      attributes.color != null &&
      (typeof attributes.color !== 'string' ||
        !/^#[0-9a-fA-F]{3,8}$/.test(attributes.color))
    )
      throw new Error('PROJECTION_COLOR_INVALID');
  }
}
function clean(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(clean);
  if (value && typeof value === 'object') {
    const result: Record<string, unknown> = {};
    for (const [key, raw] of Object.entries(value)) {
      if (key === 'attrs') {
        const a = Object.fromEntries(
          Object.entries(raw as object).filter(
            ([, v]) => v !== null && v !== undefined
          )
        );
        if (Object.keys(a).length) result[key] = clean(a);
      } else result[key] = clean(raw);
    }
    return result;
  }
  return value;
}
export function initializeOnce(value: Structured): Y.Doc {
  if (
    value.schemaVersion !== 'peanut.richtext/1' ||
    value.editorModel !== 'tiptap-prosemirror' ||
    value.annotations.length
  )
    throw new Error('INITIALIZATION_PROJECTION_INVALID');
  const doc = prosemirrorJSONToYDoc(schema, value.content, fragmentName);
  doc.gc = false;
  return doc;
}
export function project(doc: Y.Doc): {
  value: Structured;
  json: string;
  sha256: string;
  referenceDigest: string;
  fileKeys: string[];
} {
  const content = clean(
    yXmlFragmentToProsemirrorJSON(doc.getXmlFragment(fragmentName))
  ) as Record<string, unknown>;
  schema.nodeFromJSON(content).check();
  const value: Structured = {
    schemaVersion: 'peanut.richtext/1',
    editorModel: 'tiptap-prosemirror',
    content,
    annotations: [],
  };
  const json = phpJson(value, true);
  if (Buffer.byteLength(json) > 2097152)
    throw new Error('PROJECTION_TOO_LARGE');
  const refs = new Map<
    string,
    { file_key: string; variant_key: string | null }
  >();
  let count = 0;
  const visit = (node: Record<string, unknown>, depth: number): void => {
    if (++count > 50000 || depth > 60) throw new Error('PROJECTION_TOO_DEEP');
    validateAttributes(node);
    if (
      ['image', 'video', 'audio', 'file', 'media'].includes(String(node.type))
    ) {
      const a = node.attrs as Record<string, unknown>;
      if (
        !a ||
        typeof a.fileKey !== 'string' ||
        !/^file_[0-9a-f]{32}$/.test(a.fileKey)
      )
        throw new Error('PROJECTION_REFERENCE_INVALID');
      refs.set(a.fileKey, {
        file_key: a.fileKey,
        variant_key: typeof a.variantKey === 'string' ? a.variantKey : null,
      });
    }
    for (const child of (node.content ?? []) as Record<string, unknown>[])
      visit(child, depth + 1);
  };
  visit(content, 0);
  if (refs.size > 1000) throw new Error('PROJECTION_REFERENCES_TOO_LARGE');
  return {
    value,
    json,
    sha256: sha(json),
    fileKeys: [...refs.keys()].sort(),
    referenceDigest: sha(
      phpJson(
        [...refs]
          .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
          .map(([, v]) => v)
      )
    ),
  };
}

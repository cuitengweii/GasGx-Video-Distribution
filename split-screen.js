#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { spawnSync } = require('child_process');

const CANVAS_WIDTH = 1080;
const CANVAS_HEIGHT = 1920;
const DEFAULT_GAP = 6;
const DEFAULT_RANDOM_START_MAX = 3;
const DEFAULT_VIDEO_BITRATE_PRESET = 'medium';
const DEFAULT_VIDEO_CRF = '20';
const DEFAULT_AUDIO_BITRATE = '192k';
const DEFAULT_STACK_FPS = 30;
const DEFAULT_FONT_CANDIDATES = [
  'C:/Windows/Fonts/msyh.ttc',
  'C:/Windows/Fonts/msyhbd.ttc',
  'C:/Windows/Fonts/simhei.ttf',
  'C:/Windows/Fonts/simsun.ttc',
  'C:/Windows/Fonts/simsun.ttc',
  'C:/Windows/Fonts/NotoSansCJK-Regular.ttc',
];

const LAYOUT_SPECS = {
  leftRight: {
    requiredInputs: 2,
    preprocess: [
      { label: 'v0', width: 540, height: 1920 },
      { label: 'v1', width: 540, height: 1920 },
    ],
    stack: {
      inputs: ['v0', 'v1'],
      layout: '0_0|540_0',
    },
    gap: {
      vertical: { x: 540, y: 0, w: 6, h: 1920 },
    },
  },
  topBottom: {
    requiredInputs: 2,
    preprocess: [
      { label: 'v0', width: 1080, height: 960 },
      { label: 'v1', width: 1080, height: 960 },
    ],
    stack: {
      inputs: ['v0', 'v1'],
      layout: '0_0|0_960',
    },
    gap: {
      horizontal: { x: 0, y: 960, w: 1080, h: 6 },
    },
  },
  grid4: {
    requiredInputs: 4,
    preprocess: [
      { label: 'v0', width: 540, height: 960 },
      { label: 'v1', width: 540, height: 960 },
      { label: 'v2', width: 540, height: 960 },
      { label: 'v3', width: 540, height: 960 },
    ],
    stack: {
      inputs: ['v0', 'v1', 'v2', 'v3'],
      layout: '0_0|540_0|0_960|540_960',
    },
    gap: {
      vertical: { x: 540, y: 0, w: 6, h: 1920 },
      horizontal: { x: 0, y: 960, w: 1080, h: 6 },
    },
  },
  heroDetailText: {
    requiredInputs: 2,
    preprocess: [
      { label: 'hero', width: 1080, height: 1344 },
      { label: 'detail', width: 540, height: 576 },
    ],
    stack: {
      inputs: ['hero', 'detail', 'card'],
      layout: '0_0|0_1344|540_1344',
    },
    gap: {
      horizontal: { x: 0, y: 1344, w: 1080, h: 6 },
      lowerVertical: { x: 540, y: 1344, w: 6, h: 576 },
    },
  },
};

function fitVideo(inputIndex, width, height, label) {
  return (
    `[${inputIndex}:v]` +
    `scale=${width}:${height}:force_original_aspect_ratio=increase,` +
    `crop=${width}:${height},` +
    `setpts=PTS-STARTPTS,` +
    `setsar=1,` +
    `format=yuv420p[${label}]`
  );
}

function buildSplitScreenFilterComplex(options) {
  const layout = normalizeLayout(options.layout);
  const spec = LAYOUT_SPECS[layout];
  if (!spec) {
    throw new Error(`Unsupported layout "${options.layout}". Allowed layouts: ${Object.keys(LAYOUT_SPECS).join(', ')}`);
  }

  const inputs = Array.isArray(options.inputs) ? options.inputs.slice() : [];
  if (inputs.length < spec.requiredInputs) {
    throw new Error(`Layout "${layout}" requires at least ${spec.requiredInputs} input video(s), but got ${inputs.length}.`);
  }

  const selectedInputs = inputs.slice(0, spec.requiredInputs);
  const gaps = resolveGap(options.gap);
  const resolvedFontFile = resolveFontFile(options.fontFile);
  const parts = [];

  const preprocess = spec.preprocess.map((item, index) => {
    return fitVideo(index, item.width, item.height, item.label);
  });
  parts.push(...preprocess);

  if (layout === 'heroDetailText') {
    parts.push(buildHeroTextCardChain(resolvedFontFile));
  }

  parts.push(buildStackChain(layout, spec, gaps));
  parts.push(buildGapChain(layout, gaps));
  parts.push('[stacked_out]format=yuv420p[vout]');

  return {
    filterComplex: compactFilterGraph(parts),
    layout,
    selectedInputs,
    fontFile: resolvedFontFile,
    gap: gaps,
  };
}

function buildSplitScreenCommand(options) {
  const layout = normalizeLayout(options.layout);
  const spec = LAYOUT_SPECS[layout];
  if (!spec) {
    throw new Error(`Unsupported layout "${options.layout}". Allowed layouts: ${Object.keys(LAYOUT_SPECS).join(', ')}`);
  }

  const inputPaths = Array.isArray(options.inputs) ? options.inputs.slice() : [];
  if (inputPaths.length < spec.requiredInputs) {
    throw new Error(`Layout "${layout}" requires at least ${spec.requiredInputs} input video(s), but got ${inputPaths.length}.`);
  }

  const selectedInputs = inputPaths.slice(0, spec.requiredInputs);
  const warnings = inputPaths.length > spec.requiredInputs
    ? [`Layout "${layout}" only uses the first ${spec.requiredInputs} input(s); ignoring ${inputPaths.length - spec.requiredInputs} extra file(s).`]
    : [];

  selectedInputs.forEach((inputPath) => {
    if (!fs.existsSync(inputPath)) {
      throw new Error(`Input file does not exist: ${inputPath}`);
    }
  });

  const probes = selectedInputs.map((inputPath) => probeMedia(inputPath));
  const randomStart = Boolean(options.randomStart);
  const seekOffsets = probes.map((probe) => {
    if (!randomStart) {
      return 0;
    }
    return pickRandomSeekOffset(probe.durationSeconds);
  });

  const outputPath = resolveOutputPath(options.output, layout);
  ensureParentDirectory(outputPath);

  const filterResult = buildSplitScreenFilterComplex({
    inputs: selectedInputs,
    layout,
    fontFile: options.fontFile,
    gap: options.gap,
  });

  const ffmpegArgs = ['-y'];
  selectedInputs.forEach((inputPath, index) => {
    const seek = seekOffsets[index];
    if (seek > 0) {
      ffmpegArgs.push('-ss', formatSeconds(seek));
    }
    ffmpegArgs.push('-i', inputPath);
  });

  ffmpegArgs.push('-filter_complex', filterResult.filterComplex);
  ffmpegArgs.push('-map', '[vout]');

  if (probes[0].hasAudio) {
    ffmpegArgs.push('-map', '0:a:0?');
    ffmpegArgs.push('-c:a', 'aac');
    ffmpegArgs.push('-b:a', DEFAULT_AUDIO_BITRATE);
  } else {
    ffmpegArgs.push('-an');
  }

  ffmpegArgs.push(
    '-c:v',
    'libx264',
    '-preset',
    options.preset || DEFAULT_VIDEO_BITRATE_PRESET,
    '-crf',
    String(options.crf || DEFAULT_VIDEO_CRF),
    '-pix_fmt',
    'yuv420p',
    '-shortest',
    '-movflags',
    '+faststart',
    outputPath
  );

  const command = ['ffmpeg', ...ffmpegArgs];

  return {
    layout,
    outputPath,
    inputs: selectedInputs,
    warnings,
    probes,
    seekOffsets,
    fontFile: filterResult.fontFile,
    gap: filterResult.gap,
    filterComplex: filterResult.filterComplex,
    args: ffmpegArgs,
    command,
    commandString: renderCommand(command),
  };
}

function createSplitScreenVideo(options) {
  const job = buildSplitScreenCommand(options);
  printCommand(job);
  if (options.dryRun) {
    return { ...job, status: 0, dryRun: true };
  }

  const result = spawnSync('ffmpeg', job.args, {
    encoding: 'utf8',
    windowsHide: true,
    stdio: 'inherit',
  });

  if (result.error) {
    throw new Error(`Failed to execute ffmpeg: ${result.error.message}`);
  }
  if (typeof result.status === 'number' && result.status !== 0) {
    throw new Error(`ffmpeg exited with code ${result.status}`);
  }

  return { ...job, status: 0, dryRun: false };
}

function buildHeroTextCardChain(fontFile) {
  const fontArg = fontFile ? `fontfile=${escapeFilterPath(fontFile)}:` : '';
  const lines = [];

  lines.push(
    `color=c=black:s=540x576:r=${DEFAULT_STACK_FPS}:d=86400[card_base]`
  );
  lines.push(
    `[card_base]` +
      drawText(fontArg, '废气', {
        fontcolor: '#FFFFFF',
        fontsize: 58,
        x: 42,
        y: 72,
      }) +
      drawText(fontArg, '→', {
        fontcolor: '#5DD62C',
        fontsize: 58,
        x: 188,
        y: 72,
      }) +
      drawText(fontArg, '电力', {
        fontcolor: '#FFFFFF',
        fontsize: 58,
        x: 42,
        y: 166,
      }) +
      drawText(fontArg, '→', {
        fontcolor: '#5DD62C',
        fontsize: 58,
        x: 188,
        y: 166,
      }) +
      drawText(fontArg, '变现', {
        fontcolor: '#5DD62C',
        fontsize: 72,
        x: 42,
        y: 266,
      }) +
      drawText(fontArg, '70\\%', {
        fontcolor: '#5DD62C',
        fontsize: 30,
        x: 42,
        y: 408,
      }) +
      drawText(fontArg, ' 主画面 + ', {
        fontcolor: '#FFFFFF',
        fontsize: 30,
        x: 118,
        y: 408,
      }) +
      drawText(fontArg, '30\\%', {
        fontcolor: '#5DD62C',
        fontsize: 30,
        x: 284,
        y: 408,
      }) +
      drawText(fontArg, ' 细节/文案', {
        fontcolor: '#FFFFFF',
        fontsize: 30,
        x: 360,
        y: 408,
      }) +
      '[card]'
  );

  return compactFilterGraph(lines);
}

function buildStackChain(layout, spec, gap) {
  const labels = layout === 'heroDetailText'
    ? ['hero', 'detail', 'card']
    : spec.stack.inputs;
  return `[${labels.join('][')}]xstack=inputs=${labels.length}:layout=${spec.stack.layout}:shortest=1[stacked_base]`;
}

function buildGapChain(layout, gap) {
  if (gap <= 0) {
    return '[stacked_base]copy[stacked_out]';
  }

  const blocks = [];
  const line = Math.max(1, Math.floor(gap));
  const half = Math.floor(line / 2);

  if (layout === 'leftRight') {
    blocks.push(
      `[stacked_base]drawbox=x=${Math.max(0, 540 - half)}:y=0:w=${line}:h=${CANVAS_HEIGHT}:color=black@1.0:t=fill[stacked_out]`
    );
    return compactFilterGraph(blocks);
  }

  if (layout === 'topBottom') {
    blocks.push(
      `[stacked_base]drawbox=x=0:y=${Math.max(0, 960 - half)}:w=${CANVAS_WIDTH}:h=${line}:color=black@1.0:t=fill[stacked_out]`
    );
    return compactFilterGraph(blocks);
  }

  if (layout === 'grid4') {
    blocks.push(
      `[stacked_base]drawbox=x=${Math.max(0, 540 - half)}:y=0:w=${line}:h=${CANVAS_HEIGHT}:color=black@1.0:t=fill[mid_v]`
    );
    blocks.push(
      `[mid_v]drawbox=x=0:y=${Math.max(0, 960 - half)}:w=${CANVAS_WIDTH}:h=${line}:color=black@1.0:t=fill[stacked_out]`
    );
    return compactFilterGraph(blocks);
  }

  if (layout === 'heroDetailText') {
    blocks.push(
      `[stacked_base]drawbox=x=0:y=${Math.max(0, 1344 - half)}:w=${CANVAS_WIDTH}:h=${line}:color=black@1.0:t=fill[mid_h]`
    );
    blocks.push(
      `[mid_h]drawbox=x=${Math.max(0, 540 - half)}:y=1344:w=${line}:h=576:color=black@1.0:t=fill[stacked_out]`
    );
    return compactFilterGraph(blocks);
  }

  return '[stacked_base]copy[stacked_out]';
}

function drawText(fontArg, text, options) {
  const color = options.fontcolor || '#FFFFFF';
  return (
    `,drawtext=${fontArg}` +
    `text='${escapeDrawtextText(text)}':` +
    `fontcolor=${color}:` +
    `fontsize=${options.fontsize}:` +
    `x=${options.x}:` +
    `y=${options.y}:` +
    `line_spacing=0`
  );
}

function probeMedia(inputPath) {
  const result = spawnSync('ffprobe', [
    '-v',
    'error',
    '-print_format',
    'json',
    '-show_format',
    '-show_streams',
    inputPath,
  ], {
    encoding: 'utf8',
    windowsHide: true,
    maxBuffer: 10 * 1024 * 1024,
  });

  if (result.error) {
    throw new Error(`Failed to execute ffprobe: ${result.error.message}`);
  }
  if (result.status !== 0) {
    const message = (result.stderr || result.stdout || '').trim();
    throw new Error(`ffprobe failed for ${inputPath}${message ? `: ${message}` : ''}`);
  }

  let payload;
  try {
    payload = JSON.parse(result.stdout || '{}');
  } catch (error) {
    throw new Error(`Unable to parse ffprobe output for ${inputPath}: ${error.message}`);
  }

  const formatDuration = safeNumber(payload?.format?.duration, 0);
  const streams = Array.isArray(payload?.streams) ? payload.streams : [];
  const hasAudio = streams.some((stream) => String(stream?.codec_type || '') === 'audio');

  return {
    path: inputPath,
    durationSeconds: formatDuration,
    hasAudio,
  };
}

function resolveFontFile(fontFile) {
  if (fontFile) {
    const resolved = path.resolve(String(fontFile));
    if (!fs.existsSync(resolved)) {
      throw new Error(`fontFile does not exist: ${resolved}`);
    }
    return resolved;
  }

  for (const candidate of DEFAULT_FONT_CANDIDATES) {
    if (fs.existsSync(candidate)) {
      return path.resolve(candidate);
    }
  }

  return '';
}

function pickRandomSeekOffset(durationSeconds) {
  const duration = safeNumber(durationSeconds, 0);
  if (duration <= 0) {
    return 0;
  }

  const maxOffset = Math.max(0, Math.min(DEFAULT_RANDOM_START_MAX, duration - 0.5));
  if (maxOffset <= 0) {
    return 0;
  }

  return roundTo(Math.random() * maxOffset, 3);
}

function resolveOutputPath(outputPath, layout) {
  if (outputPath && String(outputPath).trim()) {
    return path.resolve(String(outputPath));
  }

  const stamp = formatTimestamp(new Date());
  return path.resolve(process.cwd(), `output_${layout}_${stamp}.mp4`);
}

function normalizeLayout(layout) {
  return String(layout || '').trim();
}

function resolveGap(rawGap) {
  const gap = safeInteger(rawGap, DEFAULT_GAP);
  return Math.max(0, gap);
}

function ensureParentDirectory(filePath) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

function compactFilterGraph(parts) {
  const normalized = parts
    .flatMap((part) => String(part || '').split('\n'))
    .map((item) => item.trim())
    .filter(Boolean)
    .join(';');

  return normalized
    .replace(/;;+/g, ';')
    .replace(/,+,/g, ',')
    .replace(/;,+/g, ';')
    .replace(/,+;/g, ';')
    .replace(/^;+|;+$/g, '');
}

function formatSeconds(value) {
  return roundTo(safeNumber(value, 0), 3).toFixed(3);
}

function formatTimestamp(date) {
  const year = String(date.getFullYear());
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hour = String(date.getHours()).padStart(2, '0');
  const minute = String(date.getMinutes()).padStart(2, '0');
  const second = String(date.getSeconds()).padStart(2, '0');
  return `${year}${month}${day}_${hour}${minute}${second}`;
}

function renderCommand(commandParts) {
  return commandParts
    .map((part) => quoteShellArg(String(part)))
    .join(' ');
}

function printCommand(job) {
  if (job.warnings && job.warnings.length > 0) {
    job.warnings.forEach((warning) => console.warn(`[split-screen] ${warning}`));
  }
  console.log('[split-screen] filter_complex:');
  console.log(job.filterComplex);
  console.log('[split-screen] command:');
  console.log(job.commandString);
}

function quoteShellArg(value) {
  if (value === '') {
    return '""';
  }
  if (/^[A-Za-z0-9_\/\\:.\-]+$/.test(value)) {
    return value;
  }
  return `"${value.replace(/"/g, '\\"')}"`;
}

function escapeFilterPath(filePath) {
  return String(path.resolve(filePath))
    .replace(/\\/g, '/')
    .replace(/:/g, '\\:')
    .replace(/'/g, "\\'");
}

function escapeDrawtextText(text) {
  return String(text)
    .replace(/\\/g, '\\\\')
    .replace(/:/g, '\\:')
    .replace(/,/g, '\\,')
    .replace(/;/g, '\\;')
    .replace(/\[/g, '\\[')
    .replace(/\]/g, '\\]')
    .replace(/'/g, "\\'")
    .replace(/%/g, '\\%');
}

function safeNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function safeInteger(value, fallback) {
  const parsed = Number.parseInt(String(value), 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function roundTo(value, digits) {
  const factor = Math.pow(10, digits);
  return Math.round(value * factor) / factor;
}

function parseBoolean(value, defaultValue = false) {
  if (value === undefined || value === null) {
    return defaultValue;
  }
  if (typeof value === 'boolean') {
    return value;
  }
  const normalized = String(value).trim().toLowerCase();
  if (!normalized) {
    return defaultValue;
  }
  if (['1', 'true', 'yes', 'y', 'on'].includes(normalized)) {
    return true;
  }
  if (['0', 'false', 'no', 'n', 'off'].includes(normalized)) {
    return false;
  }
  return defaultValue;
}

function parseCliArgs(argv) {
  const args = Array.isArray(argv) ? argv.slice() : [];
  const result = {
    layout: '',
    inputs: [],
    output: '',
    fontFile: '',
    randomStart: false,
    gap: DEFAULT_GAP,
    dryRun: false,
    preset: DEFAULT_VIDEO_BITRATE_PRESET,
    crf: DEFAULT_VIDEO_CRF,
  };

  for (let index = 0; index < args.length; index += 1) {
    const token = args[index];
    if (token === '--help' || token === '-h') {
      result.help = true;
      continue;
    }

    if (token.startsWith('--')) {
      const [flagName, inlineValue] = token.split(/=(.*)/s, 2);
      const value = inlineValue !== undefined ? inlineValue : args[index + 1];
      switch (flagName) {
        case '--output':
          result.output = inlineValue !== undefined ? inlineValue : (args[++index] || '');
          break;
        case '--fontFile':
        case '--font-file':
          result.fontFile = inlineValue !== undefined ? inlineValue : (args[++index] || '');
          break;
        case '--gap':
          result.gap = safeInteger(inlineValue !== undefined ? inlineValue : (args[++index] || DEFAULT_GAP), DEFAULT_GAP);
          break;
        case '--randomStart':
        case '--random-start':
          result.randomStart = parseBoolean(inlineValue !== undefined ? inlineValue : args[index + 1], true);
          if (inlineValue === undefined && typeof args[index + 1] === 'string' && !args[index + 1].startsWith('--')) {
            index += 1;
          }
          break;
        case '--dry-run':
        case '--dryRun':
          result.dryRun = inlineValue !== undefined ? parseBoolean(inlineValue, true) : true;
          if (inlineValue === undefined && typeof args[index + 1] === 'string' && !args[index + 1].startsWith('--')) {
            index += 1;
          }
          break;
        case '--preset':
          result.preset = inlineValue !== undefined ? inlineValue : (args[++index] || DEFAULT_VIDEO_BITRATE_PRESET);
          break;
        case '--crf':
          result.crf = inlineValue !== undefined ? inlineValue : (args[++index] || DEFAULT_VIDEO_CRF);
          break;
        case '--layout':
          result.layout = inlineValue !== undefined ? inlineValue : (args[++index] || '');
          break;
        default:
          throw new Error(`Unknown option: ${flagName}`);
      }
      continue;
    }

    if (!result.layout) {
      result.layout = token;
    } else {
      result.inputs.push(token);
    }
  }

  return result;
}

function printHelp() {
  console.log([
    'Usage:',
    '  node split-screen.js <layout> <input1> <input2> [input3] [input4] [options]',
    '',
    'Layouts:',
    '  leftRight        - 2 inputs, left/right split',
    '  topBottom        - 2 inputs, top/bottom split',
    '  grid4            - 4 inputs, 2x2 grid',
    '  heroDetailText   - 2 inputs, hero + detail + text card',
    '',
    'Options:',
    '  --output <path>        output mp4 path',
    '  --fontFile <path>      font file path for drawtext',
    '  --randomStart[=true]   randomize each input start time within 0-3 seconds',
    '  --gap <px>             black separator thickness (default: 6)',
    '  --preset <name>        x264 preset (default: medium)',
    '  --crf <value>          x264 CRF (default: 20)',
    '  --dry-run              print command and exit',
    '',
    'Examples:',
    '  node split-screen.js leftRight a.mp4 b.mp4',
    '  node split-screen.js topBottom a.mp4 b.mp4',
    '  node split-screen.js grid4 a.mp4 b.mp4 c.mp4 d.mp4',
    '  node split-screen.js heroDetailText main.mp4 detail.mp4 --fontFile C:/Windows/Fonts/msyh.ttc',
  ].join('\n'));
}

function main() {
  try {
    const parsed = parseCliArgs(process.argv.slice(2));
    if (parsed.help) {
      printHelp();
      return;
    }

    if (!parsed.layout) {
      throw new Error('Missing layout argument. Run with --help to see usage.');
    }

    const job = buildSplitScreenCommand({
      layout: parsed.layout,
      inputs: parsed.inputs,
      output: parsed.output,
      fontFile: parsed.fontFile,
      randomStart: parsed.randomStart,
      gap: parsed.gap,
      dryRun: parsed.dryRun,
      preset: parsed.preset,
      crf: parsed.crf,
    });

    printCommand(job);
    if (parsed.dryRun) {
      return;
    }

    const result = spawnSync('ffmpeg', job.args, {
      encoding: 'utf8',
      windowsHide: true,
      stdio: 'inherit',
    });

    if (result.error) {
      throw new Error(`Failed to execute ffmpeg: ${result.error.message}`);
    }
    if (typeof result.status === 'number' && result.status !== 0) {
      throw new Error(`ffmpeg exited with code ${result.status}`);
    }
  } catch (error) {
    console.error(`[split-screen] ${error.message}`);
    process.exitCode = 1;
  }
}

module.exports = {
  CANVAS_HEIGHT,
  CANVAS_WIDTH,
  DEFAULT_GAP,
  LAYOUT_SPECS,
  buildSplitScreenCommand,
  buildSplitScreenFilterComplex,
  createSplitScreenVideo,
  escapeDrawtextText,
  escapeFilterPath,
  fitVideo,
  parseCliArgs,
  probeMedia,
  resolveFontFile,
};

if (require.main === module) {
  main();
}

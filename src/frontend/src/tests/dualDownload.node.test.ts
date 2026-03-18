import * as assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { selectBestComplementaryRelease } from '../utils/dualDownload.js';
import type { Release, DualDownloadConfig } from '../types/index.js';

function buildRelease(overrides: Partial<Release>): Release {
  return {
    source: 'direct_download',
    source_id: `release-${Math.random().toString(36).slice(2, 8)}`,
    title: 'Test Book',
    ...overrides,
  };
}

const DEFAULT_CONFIG: DualDownloadConfig = {
  preferredFormat: '',
  fallbackFormat: '',
  maxSizeMb: 0,
};

describe('selectBestComplementaryRelease', () => {
  it('returns null for empty release list', () => {
    const result = selectBestComplementaryRelease([], DEFAULT_CONFIG);
    assert.equal(result, null);
  });

  it('returns the first release when no format preferences set', () => {
    const releases = [
      buildRelease({ format: 'm4b', size: '300 MB' }),
      buildRelease({ format: 'mp3', size: '200 MB' }),
    ];

    const result = selectBestComplementaryRelease(releases, DEFAULT_CONFIG);
    assert.equal(result, releases[0]);
  });

  it('selects preferred format over others', () => {
    const mp3 = buildRelease({ format: 'mp3', size: '200 MB' });
    const m4b = buildRelease({ format: 'm4b', size: '300 MB' });
    const releases = [mp3, m4b];

    const config: DualDownloadConfig = {
      preferredFormat: 'm4b',
      fallbackFormat: '',
      maxSizeMb: 0,
    };

    const result = selectBestComplementaryRelease(releases, config);
    assert.equal(result, m4b);
  });

  it('falls back to fallback format when preferred not available', () => {
    const mp3 = buildRelease({ format: 'mp3', size: '200 MB' });
    const epub = buildRelease({ format: 'epub', size: '5 MB' });
    const releases = [mp3, epub];

    const config: DualDownloadConfig = {
      preferredFormat: 'm4b',
      fallbackFormat: 'mp3',
      maxSizeMb: 0,
    };

    const result = selectBestComplementaryRelease(releases, config);
    assert.equal(result, mp3);
  });

  it('falls back to any format when neither preferred nor fallback available', () => {
    const ogg = buildRelease({ format: 'ogg', size: '150 MB' });
    const releases = [ogg];

    const config: DualDownloadConfig = {
      preferredFormat: 'm4b',
      fallbackFormat: 'mp3',
      maxSizeMb: 0,
    };

    const result = selectBestComplementaryRelease(releases, config);
    assert.equal(result, ogg);
  });

  it('filters by max size when configured', () => {
    const small = buildRelease({ format: 'm4b', size: '50 MB' });
    const large = buildRelease({ format: 'm4b', size: '500 MB' });
    const releases = [large, small];

    const config: DualDownloadConfig = {
      preferredFormat: '',
      fallbackFormat: '',
      maxSizeMb: 100,
    };

    const result = selectBestComplementaryRelease(releases, config);
    assert.equal(result, small);
  });

  it('ignores size filter when all releases exceed limit', () => {
    const large1 = buildRelease({ format: 'm4b', size: '500 MB' });
    const large2 = buildRelease({ format: 'mp3', size: '400 MB' });
    const releases = [large1, large2];

    const config: DualDownloadConfig = {
      preferredFormat: '',
      fallbackFormat: '',
      maxSizeMb: 100,
    };

    // Should still return something rather than null
    const result = selectBestComplementaryRelease(releases, config);
    assert.notEqual(result, null);
  });

  it('passes through releases with unparseable size when filtering', () => {
    const unparseable = buildRelease({ format: 'm4b', size: 'unknown' });
    const tooBig = buildRelease({ format: 'm4b', size: '500 MB' });
    const releases = [tooBig, unparseable];

    const config: DualDownloadConfig = {
      preferredFormat: '',
      fallbackFormat: '',
      maxSizeMb: 100,
    };

    const result = selectBestComplementaryRelease(releases, config);
    assert.equal(result, unparseable);
  });

  it('handles releases with no size field', () => {
    const noSize = buildRelease({ format: 'm4b' });
    const releases = [noSize];

    const config: DualDownloadConfig = {
      preferredFormat: '',
      fallbackFormat: '',
      maxSizeMb: 100,
    };

    const result = selectBestComplementaryRelease(releases, config);
    assert.equal(result, noSize);
  });

  it('handles GB size units', () => {
    const gb = buildRelease({ format: 'm4b', size: '2 GB' });
    const releases = [gb];

    const config: DualDownloadConfig = {
      preferredFormat: '',
      fallbackFormat: '',
      maxSizeMb: 1000, // 1000 MB = ~1 GB
    };

    // 2 GB > 1000 MB, should be filtered, but since it's the only one, still returned
    const result = selectBestComplementaryRelease(releases, config);
    assert.equal(result, gb);
  });

  it('applies format preference case-insensitively', () => {
    const m4b = buildRelease({ format: 'M4B', size: '300 MB' });
    const releases = [m4b];

    const config: DualDownloadConfig = {
      preferredFormat: 'm4b',
      fallbackFormat: '',
      maxSizeMb: 0,
    };

    const result = selectBestComplementaryRelease(releases, config);
    assert.equal(result, m4b);
  });

  it('uses title match scoring when candidates provided', () => {
    const goodMatch = buildRelease({ format: 'm4b', title: 'The Great Gatsby' });
    const badMatch = buildRelease({ format: 'm4b', title: 'Completely Different Book' });
    const releases = [badMatch, goodMatch];

    const result = selectBestComplementaryRelease(
      releases,
      DEFAULT_CONFIG,
      ['the great gatsby'],
      [],
    );

    assert.equal(result, goodMatch);
  });

  it('combines size filter with format preference', () => {
    const largeM4b = buildRelease({ format: 'm4b', size: '500 MB' });
    const smallMp3 = buildRelease({ format: 'mp3', size: '50 MB' });
    const smallM4b = buildRelease({ format: 'm4b', size: '80 MB' });
    const releases = [largeM4b, smallMp3, smallM4b];

    const config: DualDownloadConfig = {
      preferredFormat: 'm4b',
      fallbackFormat: 'mp3',
      maxSizeMb: 100,
    };

    // Should pick the small m4b (preferred format + under size limit)
    const result = selectBestComplementaryRelease(releases, config);
    assert.equal(result, smallM4b);
  });

  it('maxSizeMb of 0 means no limit', () => {
    const large = buildRelease({ format: 'm4b', size: '5000 MB' });
    const releases = [large];

    const config: DualDownloadConfig = {
      preferredFormat: '',
      fallbackFormat: '',
      maxSizeMb: 0,
    };

    const result = selectBestComplementaryRelease(releases, config);
    assert.equal(result, large);
  });
});

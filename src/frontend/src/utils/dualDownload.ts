import { Release, DualDownloadConfig } from '../types/index.js';
import { sortReleasesByBookMatch } from './releaseScoring.js';

/**
 * Parse a human-readable file size string (e.g., "320 MB", "1.2 GB") to megabytes.
 * Returns null if the string cannot be parsed.
 */
function parseSizeToMb(size: string | null | undefined): number | null {
  if (!size) return null;
  const match = size.trim().match(/^([\d.]+)\s*(bytes?|kb|mb|gb|tb)$/i);
  if (!match) return null;
  const value = parseFloat(match[1]);
  if (isNaN(value)) return null;
  const unit = match[2].toLowerCase().replace(/s$/, '');
  switch (unit) {
    case 'byte': return value / (1024 * 1024);
    case 'kb': return value / 1024;
    case 'mb': return value;
    case 'gb': return value * 1024;
    case 'tb': return value * 1024 * 1024;
    default: return null;
  }
}

/**
 * Select the best complementary release based on user preferences.
 *
 * Priority:
 * 1. Filter by max size (if configured)
 * 2. Try preferred format first, then fallback format, then any format
 * 3. Among matches, sort by book title/author match score
 * 4. Return the top result or null
 */
export function selectBestComplementaryRelease(
  releases: Release[],
  config: DualDownloadConfig,
  titleCandidates: string[] = [],
  authorCandidates: string[] = [],
): Release | null {
  if (releases.length === 0) return null;

  // Step 1: Filter by max size
  let candidates = releases;
  if (config.maxSizeMb > 0) {
    const sizeFiltered = candidates.filter((r) => {
      const sizeMb = parseSizeToMb(r.size);
      return sizeMb === null || sizeMb <= config.maxSizeMb;
    });
    // If all releases exceed the size limit, don't filter (let user see them)
    if (sizeFiltered.length > 0) {
      candidates = sizeFiltered;
    }
  }

  // Step 2: Try preferred format, then fallback, then any
  const pickBest = (filtered: Release[]): Release | null => {
    if (filtered.length === 0) return null;
    const sorted = titleCandidates.length > 0
      ? sortReleasesByBookMatch(filtered, titleCandidates, authorCandidates)
      : filtered;
    return sorted[0];
  };

  if (config.preferredFormat) {
    const preferred = candidates.filter(
      (r) => r.format?.toLowerCase() === config.preferredFormat.toLowerCase(),
    );
    const result = pickBest(preferred);
    if (result) return result;
  }

  if (config.fallbackFormat) {
    const fallback = candidates.filter(
      (r) => r.format?.toLowerCase() === config.fallbackFormat.toLowerCase(),
    );
    const result = pickBest(fallback);
    if (result) return result;
  }

  // Any format
  return pickBest(candidates);
}

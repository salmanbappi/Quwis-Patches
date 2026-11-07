# Quwis-Patches — Developer Guide & Continuation Reference

This document serves as the persistent architectural reference for any current or future AI pair-programming assistant and human contributors working on this repository.

---

## 1. Project Overview & Identity

- **Repository**: [https://github.com/salmanbappi/Quwis-Patches](https://github.com/salmanbappi/Quwis-Patches)
- **Author**: [@salmanbappi](https://github.com/salmanbappi)
- **Framework**: Morphe Patcher (`app.morphe.patches:2.1.0`) targeting Chess.com Android (`com.chess` 4.10.23-googleplay & 4.10.18-googleplay).
- **Morphe Source Bundle URL**:
  ```text
  https://raw.githubusercontent.com/salmanbappi/Quwis-Patches/main/patches-bundle.json
  ```

---

## 2. Hard Constraints & Workflow Rules

1. **Build Policy**:
   - **NEVER compile or build APK locally**.
   - Local code inspection, decompilation with `jadx`, and running python scripts are permitted.
   - All `.mpp` patch bundles are built remotely via GitHub Actions CI (`.github/workflows/build-mpp.yml`).
2. **Git Policy**:
   - Strictly non-interactive operations.
   - **NEVER commit or push**: `GEMINI.md`, `AGENTS.md`, `skills/`, or agent-specific configs.
3. **Release & Versioning Lifecycle**:
   Whenever patches or extensions are modified:
   1. Bump `version = X.Y.Z` in `gradle.properties`.
   2. Update `version` and `download_url` in `patches-bundle.json`.
   3. Update `version` in `patches-list.json`.
   4. Add change description in `CHANGELOG.md`.
   5. Commit and tag:
      ```bash
      git add CHANGELOG.md extensions/ gradle.properties patches-bundle.json patches-list.json patches/
      git commit -m "<type>(<scope>): <message> and bump to X.Y.Z"
      git tag vX.Y.Z
      git push origin main && git push origin vX.Y.Z
      ```
   6. GitHub Actions (`.github/workflows/build-mpp.yml`) will build `patches-X.Y.Z.mpp` and publish it as an asset on GitHub Release `vX.Y.Z`.
   7. Morphe Manager reads `patches-bundle.json` and updates the bundle cleanly without "unnamed couldn't be updated" errors.

---

## 3. Core Architecture & Known Caveats

### A. Cloud Stockfish Engine (`ApiEngine.java`)
- **Purpose**: Replaces the slow, 80 MB local Stockfish binary download (`stockfish-android-arm64-universal.tar.gz`) with cloud-backed REST queries.
- **Location**: `extensions/chesscom/src/main/java/app/noam/extension/chesscom/stockfish/ApiEngine.java`
- **Primary API**: `https://chess-api.com/v1` (POST with `{"fen": ..., "depth": 10}`).
  - **CRITICAL QUIRK**: `chess-api.com`'s validator rejects any FEN containing an en-passant square (e.g. `e3`, `d6`) with `INVALID_FEN_VALIDATION_ERROR`. `normalizeEnPassant()` MUST normalize token 3 of the FEN to `-` before querying.
- **Backup API**: `https://stockfish.online/api/s/v2.php` (GET with `?fen=...&depth=10`).
- **Engine Fallback**: In `Engine.java`, if `libstockfish.so` is absent, it automatically redirects queries to `ApiEngine`.

### B. In-Game Move Suggestions (`Analysis.java`)
- **Purpose**: Draws real-time Stockfish suggestion arrows during active gameplay boards (Live, Bots, Daily, Puzzles).
- **Location**: `extensions/chesscom/src/main/java/app/noam/extension/chesscom/stockfish/Analysis.java`
- **Rule**: `isInGameScreen(activity)` detects active board activities and enables the suggestion loop. It explicitly excludes `GameReviewActivity` so engine suggestion arrows do NOT interfere with game reviews.

### C. Game Review (`LocalAnalysisFlow.java` & `GameReviewPatch.kt`)
- **Purpose**: Provides unlimited game reviews using Lichess-style evaluation formulas (accuracy, ACPL, and move marks).
- **1-to-1 Plies Alignment (4.10.23)**:
  - In Chess.com 4.10.23, `GameReviewViewModel` zips the game's played history (`history`, size $N$) directly with `AnalyzedGameData.positions`.
  - **Do NOT prepend an unplayed starting position to `positions`** (ply 0). Doing so shifts all positions by 1, leaving move 1 paired with `playedMove == null`, which causes line 679 of `com.chess.gamereview.v2.j0.o` to crash with `NullPointerException`.
  - `positions` must strictly contain the $N$ played positions.
- **Register Calling Convention in `GameReviewPatch.kt`**:
  - `GameReviewV2V0JFingerprint` targets `com.chess.gamereview.v2.j0.o`.
  - `j0.o` is a **`public static final`** method with 4 arguments:
    - `p0` = `AnalyzedPosition`
    - `p1` = `history.f`
    - `p2` = `GameAnalysisPermissions`
    - `p3` = `boolean`
  - Registers passed to `shouldUseDummyMove` and `buildDummyMoveResult` MUST be `p0` and `p1` (not `p1` and `p2`).
- **4.10.23 Obfuscated Types**:
  - `history.i` from older versions is `history.f` in 4.10.23.
  - `variants.d` is `variants.e` in 4.10.23.
  - Move converter method on `MoveConverterKt` is `b(variants.e, String, boolean)`.
  - `AnalysisMoveClassification` enum constants are obfuscated as `b, c, d...` (not `BOOK`).

---

## 4. Quick Diagnostics Commands

```bash
# Check GitHub Actions runs
gh run list -R salmanbappi/Quwis-Patches

# Check latest release assets
gh release view -R salmanbappi/Quwis-Patches

# Verify raw patches-bundle.json
curl -s "https://raw.githubusercontent.com/salmanbappi/Quwis-Patches/main/patches-bundle.json"

# Test Primary Cloud Stockfish API
curl -s -X POST https://chess-api.com/v1 -H "Content-Type: application/json" \
  -d '{"fen": "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1", "depth": 10}'
```

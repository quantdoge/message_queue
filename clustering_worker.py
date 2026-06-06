"""Clustering worker: claims file-upload tasks from the queue and runs DBSCAN.

Pipeline per task
-----------------
1. Decode the uploaded file (CSV / Excel) into a DataFrame.
2. Separate columns into categorical and numerical.
3. Discard unhelpful categoricals (all-unique or constant).
4. Scale numericals with StandardScaler then drop low-variance ones
   (VarianceThreshold = 0.05).
5. Frequency-encode remaining categoricals (replace each value with its
   relative frequency in the column) and join with scaled numericals.
   This keeps exactly 1 column per categorical feature — no dimensionality
   explosion — and preserves rarity signal useful for audit sampling.
6. If actual post-encoding column count >= 5, apply PCA to 5 components.
7. Determine DBSCAN parameters automatically:
       min_samples = 2 × dimensions
       eps         = k-distance graph knee (KneeLocator, S=1)
8. Run DBSCAN; label outliers as "outlier".
9. Save one row per cluster into the cluster_results table:
       ticket_id, cluster label, index_list (JSON array), summary text.
   Summary is computed on original (unscaled, pre-PCA) valid feature values.
"""

import base64
import io
import json
import traceback

import numpy as np
import pandas as pd
from kneed import KneeLocator
from sklearn.cluster import DBSCAN
from sklearn.decomposition import PCA
from sklearn.feature_selection import VarianceThreshold
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler

from task_queue import TaskQueue
from worker import Worker


class ClusteringWorker(Worker):
    """Worker subclass that performs DBSCAN clustering on uploaded files."""

    def run(self) -> None:
        print(f"[{self.name}] started, waiting for clustering tasks...")
        while not self._stop.is_set():
            task = self.queue.dequeue(timeout=2)
            if task is None:
                continue
            ticket_id = task["_row_id"]
            try:
                cluster_rows = self.process(task)
                self.queue.save_cluster_results(ticket_id, cluster_rows)
                self.queue.succeed(task)
                print(f"[{self.name}] ticket {ticket_id} succeeded ({len(cluster_rows)} clusters)")
            except Exception as exc:
                self.queue.fail(task, traceback.format_exc())
                print(f"[{self.name}] ticket {ticket_id} failed: {exc}")
        print(f"[{self.name}] stopped.")

    def process(self, task: dict) -> list[dict]:  # type: ignore[override]
        """Run the full clustering pipeline and return per-cluster result rows."""
        file_bytes = base64.b64decode(task["file_data"])
        filename = task["filename"]

        # ------------------------------------------------------------------
        # a. Load into DataFrame
        # ------------------------------------------------------------------
        if filename.lower().endswith(".csv"):
            df = pd.read_csv(io.BytesIO(file_bytes))
        else:
            df = pd.read_excel(io.BytesIO(file_bytes))

        n_rows = len(df)
        if n_rows < 2:
            raise ValueError("Dataset must have at least 2 rows.")

        # ------------------------------------------------------------------
        # b. Separate and filter features
        # ------------------------------------------------------------------
        cat_cols = df.select_dtypes(include=["object", "category", "bool"]).columns.tolist()
        num_cols = df.select_dtypes(include=["number"]).columns.tolist()

        # Categorical: keep only columns that are neither constant nor all-unique
        valid_cat = [
            col for col in cat_cols
            if 1 < df[col].nunique() < n_rows
        ]

        # Numerical: scale then variance-filter
        scaled_num_df = pd.DataFrame()
        valid_num: list[str] = []
        if num_cols:
            medians = df[num_cols].median()
            filled = df[num_cols].fillna(medians)
            scaler = StandardScaler()
            scaled = scaler.fit_transform(filled)
            scaled_num_df = pd.DataFrame(scaled, columns=num_cols)

            selector = VarianceThreshold(threshold=0.05)
            selector.fit(scaled_num_df)
            valid_num = [col for col, keep in zip(num_cols, selector.get_support()) if keep]
            scaled_num_df = scaled_num_df[valid_num]

        if len(valid_cat) + len(valid_num) == 0:
            raise ValueError("No valid features remain after filtering.")

        # ------------------------------------------------------------------
        # Build feature matrix — frequency-encode categoricals
        # Each category value is replaced by its relative frequency in that
        # column (value_counts normalised). This produces exactly 1 column
        # per categorical feature, avoiding the dimensionality explosion of
        # one-hot encoding and preserving rarity signal for audit sampling.
        # ------------------------------------------------------------------
        parts: list[pd.DataFrame] = []
        if valid_num:
            parts.append(scaled_num_df.reset_index(drop=True))
        if valid_cat:
            freq_df = df[valid_cat].copy().reset_index(drop=True)
            for col in valid_cat:
                freq_map = freq_df[col].value_counts(normalize=True)
                freq_df[col] = freq_df[col].map(freq_map).astype(float)
            parts.append(freq_df)

        X = pd.concat(parts, axis=1).values.astype(float)

        # ------------------------------------------------------------------
        # PCA: reduce to 5 dimensions when post-encoding column count >= 5
        # ------------------------------------------------------------------
        if X.shape[1] >= 5:
            n_components = min(5, X.shape[1], n_rows - 1)
            X = PCA(n_components=n_components).fit_transform(X)
            n_dims = n_components
        else:
            n_dims = X.shape[1]

        # ------------------------------------------------------------------
        # c. min_samples = 2 × dimensions
        # ------------------------------------------------------------------
        min_samples = max(2, 2 * n_dims)

        # ------------------------------------------------------------------
        # d. eps via k-distance graph + KneeLocator (S=1)
        # ------------------------------------------------------------------
        k = min(min_samples, n_rows - 1)
        nbrs = NearestNeighbors(n_neighbors=k).fit(X)
        distances, _ = nbrs.kneighbors(X)
        k_distances = np.sort(distances[:, -1])

        knee = KneeLocator(
            x=list(range(len(k_distances))),
            y=k_distances.tolist(),
            curve="convex",
            direction="increasing",
            S=1.0,
        )
        eps = (
            float(k_distances[knee.knee])
            if knee.knee is not None
            else float(np.median(k_distances))
        )
        eps = max(eps, 1e-9)

        # ------------------------------------------------------------------
        # e. DBSCAN
        # ------------------------------------------------------------------
        labels = DBSCAN(eps=eps, min_samples=min_samples).fit_predict(X)

        # ------------------------------------------------------------------
        # f. Build per-cluster DB rows (summaries use original unscaled values)
        # ------------------------------------------------------------------
        return self._build_cluster_rows(df, labels, valid_cat, valid_num)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_cluster_rows(
        self,
        df: pd.DataFrame,
        labels: np.ndarray,
        valid_cat: list[str],
        valid_num: list[str],
    ) -> list[dict]:
        """Return one dict per cluster, ready to insert into cluster_results."""
        rows = []
        for label in sorted(set(labels)):
            mask = labels == label
            indices = [int(i) for i in np.where(mask)[0]]
            cluster_label = "outlier" if label == -1 else str(int(label))
            cluster_df = df.iloc[indices]
            summary = self._summarize_cluster(cluster_label, cluster_df, valid_cat, valid_num)
            rows.append({
                "cluster": cluster_label,
                "index_list": json.dumps(indices),
                "summary": summary,
            })
        return rows

    def _summarize_cluster(
        self,
        cluster_label: str,
        cluster_df: pd.DataFrame,
        valid_cat: list[str],
        valid_num: list[str],
    ) -> str:
        """Build a human-readable summary of a cluster using original feature values."""
        n = len(cluster_df)
        heading = (
            f"Outlier group — {n} record{'s' if n != 1 else ''}"
            if cluster_label == "outlier"
            else f"Cluster {cluster_label} — {n} record{'s' if n != 1 else ''}"
        )
        lines = [heading]

        if valid_cat:
            lines.append("\nCategorical features:")
            for col in valid_cat:
                counts = cluster_df[col].value_counts(normalize=True)
                top = ", ".join(
                    f'"{v}" ({p * 100:.1f}%)' for v, p in counts.head(5).items()
                )
                lines.append(f"  {col}: {top}")

        if valid_num:
            lines.append("\nNumerical features:")
            for col in valid_num:
                s = cluster_df[col].dropna().describe()
                lines.append(
                    f"  {col}: mean={s['mean']:.2f}, std={s['std']:.2f}, "
                    f"min={s['min']:.2f}, max={s['max']:.2f}"
                )

        return "\n".join(lines)


if __name__ == "__main__":
    worker = ClusteringWorker(name="clustering-worker-standalone", queue=TaskQueue())
    try:
        worker.run()
    except KeyboardInterrupt:
        worker.stop()
        print("\nInterrupted, shutting down.")

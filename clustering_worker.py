"""Clustering worker: claims file-upload tasks from the queue and runs DBSCAN.

Pipeline per task
-----------------
1. Decode the uploaded file (CSV / Excel) into a DataFrame.
2. Separate columns into categorical and numerical.
3. Discard unhelpful categoricals (all-unique or constant).
4. Scale numericals with StandardScaler then drop low-variance ones
   (VarianceThreshold = 0.05).
5. One-hot-encode remaining categoricals and join with scaled numericals.
6. If total valid feature count >= 5, apply PCA to 5 components.
7. Determine DBSCAN parameters automatically:
       min_samples = 2 × dimensions
       eps         = k-distance graph knee (KneeLocator, S=1)
8. Run DBSCAN; label outliers as "outlier".
9. Return JSON list of {row_id, cluster} dicts.
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
            try:
                result_json = self.process(task)
                self.queue.succeed(task, result_json)
                print(f"[{self.name}] ticket {task['_row_id']} succeeded")
            except Exception as exc:
                error_msg = traceback.format_exc()
                self.queue.fail(task, error_msg)
                print(f"[{self.name}] ticket {task['_row_id']} failed: {exc}")
        print(f"[{self.name}] stopped.")

    def process(self, task: dict) -> str:  # type: ignore[override]
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

        n_valid_features = len(valid_cat) + len(valid_num)
        if n_valid_features == 0:
            raise ValueError("No valid features remain after filtering.")

        # ------------------------------------------------------------------
        # Build feature matrix
        # ------------------------------------------------------------------
        parts: list[pd.DataFrame] = []
        if valid_num:
            parts.append(scaled_num_df.reset_index(drop=True))
        if valid_cat:
            dummies = pd.get_dummies(
                df[valid_cat].reset_index(drop=True), drop_first=False
            )
            parts.append(dummies.astype(float))

        X = pd.concat(parts, axis=1).values.astype(float)

        # ------------------------------------------------------------------
        # PCA: reduce to 5 dimensions when there are >= 5 valid features
        # ------------------------------------------------------------------
        if n_valid_features >= 5:
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
        # f. Build result JSON
        # ------------------------------------------------------------------
        result = [
            {"row_id": i, "cluster": "outlier" if label == -1 else int(label)}
            for i, label in enumerate(labels)
        ]
        return json.dumps(result)


if __name__ == "__main__":
    worker = ClusteringWorker(name="clustering-worker-standalone", queue=TaskQueue())
    try:
        worker.run()
    except KeyboardInterrupt:
        worker.stop()
        print("\nInterrupted, shutting down.")

import geopandas as gpd
import numpy as np
from sklearn import metrics

from src.constants import UKRAINE_WAR_START


def get_metrics(
    gdf: gpd.GeoDataFrame,
    threshold: float = 0.5,
    method: str = "date-wise",
    print_classification_report: bool = False,
    only_2022_for_pos: bool = False,
    digits: int = 2,
    return_preds: bool = False,
) -> dict[str, float] | tuple[dict[str, float], np.ndarray, np.ndarray]:
    """
    Get the metrics for the given GeoDataFrame.

    Compute the metrics from the predictions in the GeoDataFrame and the given threshold. The method can
    be "date-wise", "date-wise-mean" or "date-wise-median". If date-wise, the threshold is applied to each
    date separately. If date-wise-mean/median, the threshold is applied to the mean/median of the
    predictions for each date.

    Args:

        gdf (gpd.GeoDataFrame): The GeoDataFrame with the predictions.
        threshold (float): The threshold to use for the classification. Defaults to 0.5.
        method (str): The method to use for the classification. Defaults to "date-wise".
        print_classification_report (bool): Whether to print the classification report. Defaults to False.
        only_2022_for_pos (bool): Whether to use only 2022 dates for the positive class. Defaults to False.
        digits (int): The number of digits to use for the classification report. Defaults to 2.
        return_preds (bool): Whether to return the predictions. Defaults to False.

    Returns:
        dict[str, float] | tuple[dict[str, float], np.ndarray, np.ndarray]: The metrics (with the preds and labels).
    """

    col_dates = [c for c in gdf.columns if c.startswith("pred_")]
    col_neg = [c for c in col_dates if c.split("_")[-1] < UKRAINE_WAR_START]
    threshold *= 255

    y_preds, y_trues = np.array([]), np.array([])
    for date, grp in gdf.groupby("date"):
        # only takes predictions after date of UNOSAT (effectively discard unknown labels)
        col_pos = [c for c in col_dates if c.split("_")[-1] >= date.strftime("%Y-%m-%d")]
        if only_2022_for_pos:
            col_pos = [c for c in col_pos if c.split("_")[-1].startswith("2022")]

        if method == "date-wise":
            y_pred_pos = (grp[col_pos] >= threshold).astype(int).values.flatten()
            y_pred_neg = (grp[col_neg] >= threshold).astype(int).values.flatten()
        elif method == "date-wise-mean":
            y_pred_pos = (grp[col_pos].mean(axis=1) >= threshold).astype(int).values
            y_pred_neg = (grp[col_neg].mean(axis=1) >= threshold).astype(int).values
        elif method == "date-wise-median":
            y_pred_pos = (grp[col_pos].median(axis=1) >= threshold).astype(int).values
            y_pred_neg = (grp[col_neg].median(axis=1) >= threshold).astype(int).values
        else:
            assert 0

        y_pred = np.concatenate([y_pred_pos, y_pred_neg])
        y_true = np.concatenate([np.ones(y_pred_pos.size), np.zeros(y_pred_neg.size)])
        y_preds = np.concatenate([y_preds, y_pred])
        y_trues = np.concatenate([y_trues, y_true])

    if print_classification_report:
        print(
            metrics.classification_report(
                y_trues,
                y_preds,
                labels=[0, 1],
                target_names=["no damage", "damaged"],
                zero_division=0,
                digits=digits,
            )
        )

    precision = metrics.precision_score(y_trues, y_preds, labels=[0, 1], zero_division=0)
    recall = metrics.recall_score(y_trues, y_preds, labels=[0, 1], zero_division=0)
    f1 = metrics.f1_score(y_trues, y_preds, labels=[0, 1], zero_division=0)
    accuracy = metrics.accuracy_score(y_trues, y_preds)
    roc_auc = metrics.roc_auc_score(y_trues, y_preds)

    d_metrics = {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "roc_auc": roc_auc,
        "accuracy": accuracy,
    }
    if return_preds:
        return d_metrics, y_preds, y_trues
    else:
        return d_metrics

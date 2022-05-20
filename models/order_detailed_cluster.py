"""Cluster orders and upload to data warehouse
Packages:
 - kmodes
"""

import pandas as pd
from kmodes.kmodes import KModes


df: pd.DataFrame = ref("order_detailed")
df_train = df[["size", "is_vegan", "is_vegetarian", "is_keto", "shape"]]

km_2 = KModes(n_clusters=3, init="Huang")
km_2.fit_predict(df_train)
df["cluster_label"] = km_2.labels_

print(df)

write_to_model(df, mode="overwrite")

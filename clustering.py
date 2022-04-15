import pandas as pd
from kmodes.kmodes import KModes


df = ref("order_detailed")
columns = ["size", "is_vegan", "is_vegetarian", "is_keto", "shape"]
df_train = df[columns]

km_2 = KModes(n_clusters=3, init="Huang")
km_2.fit_predict(df_train)
df["cluster_label"] = km_2.labels_

print(df)

write_to_source(df, "result", "order_detailed", mode="overwrite")

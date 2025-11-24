import pandas as pd
import os

"""
goal: use variable_topic_reference_us.txt
      assign topic for us10-us13 variables

output: tv files
"""

# read in topic variables reference
df_ref = pd.read_csv("variable_topic_reference_us/variable_topic_reference_us.txt", sep="\t")
# Convert to dictionary
tv_dict = dict(zip(df_ref["VariableStem"], df_ref["TopicID"]))

prefix_dict = {
               "us10": "us10_j_",
               "us11": "us11_k_",
               "us12": "us12_l_",
               "us13": "us13_m_"
              }

# read in cleaned all us topic variables from output of find_historical_variable_topic.py
df = pd.read_csv("all_us_topic_variable_filtered.txt", sep="\t")

# subset to us10 - us13
# The list of values to filter by
desired_waves = ["us10", "us11", "us12", "us13"]
df_sub = df[df["Wave"].isin(desired_waves)]

output_path = "tv"
if not os.path.exists(output_path):
    os.makedirs(output_path)

# go over wave 10 - 13
for us in desired_waves:
    # make a directory for each wave
    output_path = os.path.join("tv", us)
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    # subset
    df_us = df_sub[df_sub["Wave"] == us]
    dataset_list = df_us["DataSetName"].unique().tolist()

    # go though each dataset
    for dataset in dataset_list:
        file_name = prefix_dict[us] + dataset + "_tv.txt"

        # for each variables, assign a topic
        df_dataset = df_us[df_us["DataSetName"] == dataset]
        df_keep = df_dataset[["VariableStem"]].sort_values(by="VariableStem")
        # leading
        df_keep["DataSetPrefix"] = prefix_dict[us] + dataset
        df_keep["VariableName"] = prefix_dict[us].replace(us + "_", "") + df_keep["VariableStem"]

        # get the topic ID from tv dictionary
        df_keep["TopicID"] = df_keep["VariableStem"].map(tv_dict)

        # Convert the column to nullable integer type
        df_keep["TopicID"] = df_keep["TopicID"].astype(pd.Int64Dtype())

        df_keep[["DataSetPrefix", "VariableName", "TopicID"]].to_csv(os.path.join(output_path, file_name), sep="\t", header=False, index=False)


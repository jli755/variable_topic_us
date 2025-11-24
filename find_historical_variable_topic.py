import pandas as pd
import os

"""
goal: use variable-topic table us1 - us9
      and updated values from excel sheet

output: reference table that will be used to update us10-us13 variables
"""

# read in all us topic variables
df = pd.read_csv("archivist_tv/export_all_txt/all_us_topic_variable.txt", sep="\t")

# subset to prefix starts with us
# df_us = df.loc[df["DataSetPrefix"].str.startswith("us", na=False)]

# Replace us* with "" in the "DataSetPrefix" column
def get_data_name(text_string):
    """
    remove leading us1_a_
    """
    # Creating a dictionary with initial key-value pairs
    prefix_dict = {"us1": "us1_a_",
                   "us2": "us2_b_",
                   "us3": "us3_c_",
                   "us4": "us4_d_",
                   "us5": "us5_e_",
                   "us6": "us6_f_",
                   "us7": "us7_g_",
                   "us8": "us8_h_",
                   "us9": "us9_i_",
                   "us10": "us10_j_",
                   "us11": "us11_k_",
                   "us12": "us12_l_",
                   "us13": "us13_m_",
                   "us1_covid": "ca_",
                   "us2_covid": "cb_",
                   "us3_covid": "cc_",
                   "us4_covid": "cd_",
                   "us5_covid": "ce_",
                   "us6_covid": "cf_",
                   "us7_covid": "cg_",
                   "us8_covid": "ch_",
                   "us9_covid": "ci_"
                  }
    wave = ""
    for key, value in prefix_dict.items():
        if text_string.startswith(value):
            text_string = text_string.replace(value, "")
            wave = key

    return wave, text_string

# Replace a_* with "" in the "VariableName" column
def get_variable_stem(text_string):
    """
    remove leading a_
    """
    prefix = ["a_", "b_", "c_", "d_", "e_", "f_", "g_", "h_", "i_", "j_",
              "k_", "l_", "m_",
              "ca_", "cb_", "cc_", "cd_", "ce_", "cf_", "cg_", "ch_", "ci_",
              "a_youth.", "b_youth.", "c_youth.", "d_child.", "d_youth.",
              "e_child.", "e_indresp.", "e_youth.", "j_indresp.", "k_indresp.",
              "m_callrec.", "m_chcare.", "m_child.", "m_chmain.", "m_egoalt.",
              "m_hhresp.", "m_hhsamp.", "m_income.", "m_indall.", "m_indsamp.",
              "m_newborn.", "m_parstyle.", "m_youth.",
              "us10_j_indresp.", "us11_k_indresp.",
              "child.", "indresp.", "youth."]

    for item in prefix:
        if text_string.startswith(item):
            text_string = text_string.replace(item, "")

    return text_string

df[["Wave", "DataSetName"]] = df["DataSetPrefix"].apply(get_data_name).apply(pd.Series)
df["VariableStem"] = df["VariableName"].apply(get_variable_stem).apply(pd.Series)

# output unique variable / topic pairs
# output seperatly if multiple topics for same variable

df.to_csv("all_us_topic_variable_filtered.txt", sep="\t", index=False)

# remove TopicID = 0
df_rm0 = df[df["TopicID"] != 0]

# subset to us1 - us9
# The list of values to filter by
desired_waves = ["us1", "us2", "us3", "us4", "us5", "us6", "us7", "us8", "us9"]
df_sub = df_rm0[df_rm0["Wave"].isin(desired_waves)]

# Remove duplicates based on "VariableStem" and "TopicID", keeping the last occurrence
df_filtered = df_sub.drop_duplicates(subset=["VariableStem", "TopicID"], keep="last")

# all
df_output = df_filtered[["VariableStem", "TopicID"]].sort_values(by="VariableStem")
# df_output.to_csv("us1-us9_dictionary.txt", sep="\t", index=False)

# Remove all occurrences of values that are duplicated (keep only truly unique rows)
df_unique = df_output.drop_duplicates(subset=["VariableStem"], keep=False)
df_unique.to_csv("us1-us9_dictionary_unique.txt", sep="\t", index=False)

# output duplicated values to check
duplicate_values = df_output[df_output["VariableStem"].duplicated(keep=False)]
# merge back to the original file
merged_df = pd.merge(duplicate_values, df_sub, on=["VariableStem", "TopicID"], how="left")
merged_df.to_csv("duplicate_VariableStem_topic_from_all.txt", sep="\t", index=False)

# remove variables from updated file, then add updated variables

df_update = pd.read_csv("update_dictionary_unique.txt", sep="\t")

df_part1 = df_unique[~df_unique["VariableStem"].isin(df_update["VariableStem"])]

# Rename columns in df_update
df_update.rename(columns={"New topic": "TopicID"}, inplace=True)

# Vertical Concatenation (Adding Rows)
df_new = pd.concat([df_part1, df_update], ignore_index=True)


output_path = "variable_topic_reference_us"

if not os.path.exists(output_path):
    os.makedirs(output_path)

df_new.to_csv(os.path.join(output_path, "variable_topic_reference_us.txt"), sep="\t", index=False)

import pandas as pd

"""
- find variable stem from updated excel sheet
- get a 1-to-1 relation between variable and topic
"""

# read in all us topic variables
df = pd.read_csv("update/update-usoc-topics.csv", sep="\t")

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
              "ca_", "cb_", "cc_", "cd_", "ce_", "cf_", "cg_", "ch_", "ci_"]

    for item in prefix:
        if text_string.startswith(item):
            text_string = text_string.replace(item, "")

    return text_string

df[["Wave", "DataSetName"]] = df["Dataset"].apply(get_data_name).apply(pd.Series)
df["VariableStem"] = df["Variable"].apply(get_variable_stem).apply(pd.Series)

# output unique variable / topic pairs
# output seperatly if multiple topics for same variable

# Remove duplicates based on "VariableStem" and "TopicID", keeping the last occurrence
df_filtered = df.drop_duplicates(subset=["VariableStem", "New topic"], keep="last")

# all
df_output = df_filtered[["VariableStem", "New topic"]].sort_values(by="VariableStem")
df_output.to_csv("update_dictionary.txt", sep="\t", index=False)

# Remove all occurrences of values that are duplicated (keep only truly unique rows)
df_unique = df_output.drop_duplicates(subset=["VariableStem"], keep=False)
df_unique.to_csv("update_dictionary_unique.txt", sep="\t", index=False)

# Get all duplicate values in "VariableStem"
duplicate_values = df_output[df_output["VariableStem"].duplicated(keep=False)]
# merge back to the original file
merged_df = pd.merge(duplicate_values, df, on=["VariableStem", "New topic"], how="left")
# output for Jon to check
merged_df.to_csv("duplicate_VariableStem_topic_from_update.txt", sep="\t", index=False)

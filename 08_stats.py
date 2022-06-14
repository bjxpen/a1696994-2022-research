import pandas as pd
import scipy.stats

annotated_df = pd.read_csv("09_annotated_norm_only.csv")
untyped_df = pd.read_csv("09_untyped_norm_only.csv")

annotated_pylint_convention_msg_norm = annotated_df.loc[:,"pylint_convention_msg_norm"]
untyped_pylint_convention_msg_norm = untyped_df.loc[:,"pylint_convention_msg_norm"]

annotated_pylint_refactor_msg_norm = annotated_df.loc[:,"pylint_refactor_msg_norm"]
untyped_pylint_refactor_msg_norm = untyped_df.loc[:,"pylint_refactor_msg_norm"]

annotated_pylint_warning_msg_norm = annotated_df.loc[:,"pylint_warning_msg_norm"]
untyped_pylint_warning_msg_norm = untyped_df.loc[:,"pylint_warning_msg_norm"]

annotated_pylint_error_msg_norm = annotated_df.loc[:,"pylint_error_msg_norm"]
untyped_pylint_error_msg_norm = untyped_df.loc[:,"pylint_error_msg_norm"]

annotated_cyclomatic_complexity_norm = annotated_df.loc[:,"cyclomatic_complexity_norm"]
untyped_cyclomatic_complexity_norm = untyped_df.loc[:,"cyclomatic_complexity_norm"]

annotated_cognitive_complexity_norm = annotated_df.loc[:,"cognitive_complexity_norm"]
untyped_cognitive_complexity_norm = untyped_df.loc[:,"cognitive_complexity_norm"]

annotated_bug_score_norm = annotated_df.loc[:,"bug_score_norm"]
untyped_bug_score_norm = untyped_df.loc[:,"bug_score_norm"]


def test(prop_name, col1, col2, side):
    print("-"*20)
    print(prop_name)
    # Check if same distribution
    stat_ks, p_ks = scipy.stats.ks_2samp(col1, col2, alternative="two-sided")
    print(f"ks_2samp --> stat: {stat_ks}, p: {p_ks}")

    is_first_dist_norm = False
    is_second_dist_norm = False

    stat_norm_first, p_norm_first = scipy.stats.shapiro(col1)
    
    if p_norm_first < 0.05:
        print("Column 1 not Gaussian", end=" ")
    else:
        is_first_dist_norm = True
        print("Column 1 looks Gaussian", end=" ")
    print(stat_norm_first, f"p={p_norm_first}")

    stat_norm_second, p_norm_second = scipy.stats.shapiro(col2)
    
    if p_norm_second < 0.05:
        print("Column 2 not Gaussian", end=" ")
    else:
        is_second_dist_norm = True
        print("Column 2 looks Gaussian", end=" ")

    print(stat_norm_second, f"p={p_norm_second}")

    if is_second_dist_norm and is_first_dist_norm:
        res = scipy.stats.ttest_ind(col1, col2, alternative=side)
    else:
        res = scipy.stats.mannwhitneyu(col1, col2, alternative=side)

    print(res)
    print("p = " + str(float("{0:.32f}".format(res.pvalue))))
    print("p < 0.05: ", res.pvalue < 0.05)

if __name__ == "__main__":
    # test("pylint_convention_msg_nor", untyped_pylint_convention_msg_norm, annotated_pylint_convention_msg_norm, "greater")
    test("pylint_refactor_msg_norm", untyped_pylint_refactor_msg_norm, annotated_pylint_refactor_msg_norm, "greater")
    test("pylint_warning_msg_norm", untyped_pylint_warning_msg_norm, annotated_pylint_warning_msg_norm, "greater")
    test("pylint_error_msg_norm", untyped_pylint_error_msg_norm, annotated_pylint_error_msg_norm, "greater")
    test("cognitive_complexity_norm", untyped_cognitive_complexity_norm, annotated_cognitive_complexity_norm, "greater")
    test("cyclomatic_complexity_norm", untyped_cyclomatic_complexity_norm, annotated_cyclomatic_complexity_norm, "greater")
    test("bug_score_norm", untyped_bug_score_norm, annotated_bug_score_norm, "greater")

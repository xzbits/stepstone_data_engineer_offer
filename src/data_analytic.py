import connect_db
import sql_analysis_queries
import matplotlib.pylab as plt


def score_recruitment_chance(company_size):
    """
    Score an offer based on company_size
    :param company_size: 1 Smallest - 6 Biggest
    :return: offer score
    """
    company_size = company_size if company_size is not None else 1
    return 0.5 if company_size <= 3 else 1


if __name__ == "__main__":
    conn, cur = connect_db.get_db_connection(db_config_filepath='db.cfg', section='stepstone')
    cur.execute(sql_analysis_queries.analyze_query)
    score_dict = {}
    for one_row in cur.fetchall():
        if one_row[0] is None:
            continue
        if one_row[0] in score_dict.keys():
            score_dict[one_row[0]] = score_dict[one_row[0]] + score_recruitment_chance(one_row[1])
        else:
            score_dict[one_row[0]] = score_recruitment_chance(one_row[1])

    for k, v in sorted(score_dict.items(), key=lambda item: item[1]):
        print(k, v)

    # lists = sorted_score_dict.items()
    # x, y = zip(*lists)
    # plt.bar(x[:20], y[:20])
    # plt.show()

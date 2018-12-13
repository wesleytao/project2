import click
from google.cloud import bigquery

uni1 = 'wt2271' # Your uni
uni2 = 'alm2263' # Partner's uni. If you don't have a partner, put None

# Test function
def testquery(client):
    q = """select * from `w4111-columbia.graph.tweets` limit 3"""
    job = client.query(q)

    # waits for query to execute and return
    results = job.result()
    return list(results)

# SQL query for Question 1. You must edit this funtion.
# This function should return a list of IDs and the corresponding text.
def q1(client):
    q = """select idx, text from `w4111-columbia.graph.tweets` where text like '%www.twitch%' and text like '%going live%' """
    job = client.query(q)
    results = job.result()
    return list(results)

# SQL query for Question 2. You must edit this funtion.
# This function should return a list of days and their corresponding average likes.
def q2(client):
    q = """
    select substr(create_time,1,3) as day, avg(like_num) as avg_likes from `w4111-columbia.graph.tweets` group by substr(create_time,1,3) limit 5

    """
    job = client.query(q)
    results = job.result()
    return list(results)

def save_q3(q, client):
    # client = bigquery.Client()
    dataset_id = 'dataset'

    job_config = bigquery.QueryJobConfig()
    # Set use_legacy_sql to True to use legacy SQL syntax.
    job_config.use_legacy_sql = False
    # Set the destination table
    table_ref = client.dataset(dataset_id).table('GRAPH')
    job_config.destination = table_ref
    job_config.allow_large_results = True
    sql = q

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location='US',
        job_config=job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    print('Query results loaded to table {}'.format(table_ref.path))

# SQL query for Question 3. You must edit this funtion.
# This function should return a list of source nodes and destination nodes in the graph.
def q3(client):
    # q = """
    # select distinct twitter_username as scr, REGEXP_EXTRACT(text, r'@([^\s]*)') as dst
    # from `w4111-columbia.graph.tweets`
    # where REGEXP_CONTAINS(text, r'@[^\s]*');
    # """
    # save_q3(q, client)
    # Query results loaded to table /projects/w4111-project-2-225304/datasets/dataset/tables/GRAPH
    # print("hello ")
    q_see = """select * from dataset.GRAPH limit 5"""
    job = client.query(q_see)
    results = job.result()
    return list(results)

# SQL query for Question 4. You must edit this funtion.
# This function should return a list containing the twitter username of the users having the max indegree and max outdegree.
def q4(client):
    q = """
    with outdegree as (select scr, count(*) as cnt from dataset.GRAPH group by scr),
    indegree as (select dst, count(*) as cnt from dataset.GRAPH group by dst)
    select W1.twitter_username as max_indegree, W2.twitter_username as max_outdegree
    from `w4111-columbia.graph.tweets` as W1, `w4111-columbia.graph.tweets` as W2, indegree, outdegree
    where W1.twitter_username = indegree.dst and
    W2.twitter_username = outdegree.scr and
    indegree.cnt = (select max(cnt) from indegree) and
    outdegree.cnt = (select max(cnt) from outdegree)
    """
    job = client.query(q)
    results = job.result()
    return list(results)

# SQL query for Question 5. You must edit this funtion.
# This function should return a list containing value of the conditional probability.
def q5(client):

    job = client.query(q)
    results = job.result()
    return list(results)

# SQL query for Question 6. You must edit this funtion.
# This function should return a list containing the value for the number of triangles in the graph.
def q6(client):

    return []

# SQL query for Question 7. You must edit this funtion.
# This function should return a list containing the twitter username and their corresponding PageRank.
def q7(client):

    return []


# Do not edit this function. This is for helping you develop your own iterative PageRank algorithm.
def bfs(client, start, n_iter):

    # You should replace dataset.bfs_graph with your dataset name and table name.
    q1 = """
        CREATE TABLE IF NOT EXISTS dataset.bfs_graph (src string, dst string);
        """
    q2 = """
        INSERT INTO dataset.bfs_graph(src, dst) VALUES
        ('A', 'B'),
        ('A', 'E'),
        ('B', 'C'),
        ('C', 'D'),
        ('E', 'F'),
        ('F', 'D'),
        ('A', 'F'),
        ('B', 'E'),
        ('B', 'F'),
        ('A', 'G'),
        ('B', 'G'),
        ('F', 'G'),
        ('H', 'A'),
        ('G', 'H'),
        ('H', 'C'),
        ('H', 'D'),
        ('E', 'H'),
        ('F', 'H');
        """

    job = client.query(q1)
    results = job.result()
    job = client.query(q2)
    results = job.result()

    # You should replace dataset.distances with your dataset name and table name.
    q3 = """
        CREATE OR REPLACE TABLE dataset.distances AS
        SELECT '{start}' as node, 0 as distance
        """.format(start=start)
    job = client.query(q3)
    # Result will be empty, but calling makes the code wait for the query to complete
    job.result()

    for i in range(n_iter):
        print("Step %d..." % (i+1))
        q1 = """
        INSERT INTO dataset.distances(node, distance)
        SELECT distinct dst, {next_distance}
        FROM dataset.bfs_graph
            WHERE src IN (
                SELECT node
                FROM dataset.distances
                WHERE distance = {curr_distance}
                )
            AND dst NOT IN (
                SELECT node
                FROM dataset.distances
                )
            """.format(
                curr_distance=i,
                next_distance=i+1
            )
        job = client.query(q1)
        results = job.result()
        # print(results)





# Do not edit this function. You can use this function to see how to store tables using BigQuery.
def save_table():
    client = bigquery.Client()
    dataset_id = 'dataset'

    job_config = bigquery.QueryJobConfig()
    # Set use_legacy_sql to True to use legacy SQL syntax.
    job_config.use_legacy_sql = True
    # Set the destination table
    table_ref = client.dataset(dataset_id).table('test')
    job_config.destination = table_ref
    job_config.allow_large_results = True
    sql = """select * from [w4111-columbia.graph.tweets] limit 3"""

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location='US',
        job_config=job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    print('Query results loaded to table {}'.format(table_ref.path))

@click.command()
# @click.argument("PATHTOCRED", type=click.Path(exists=True))
def main():
    pathtocred = "./credential/w4111-project-2-1c4ce5c2b73b.json"
    client = bigquery.Client.from_service_account_json(pathtocred)

    # funcs_to_test = [q1, q2, q3, q4, q5, q6, q7]
    funcs_to_test = [q4]
    # funcs_to_test = [testquery]
    for func in funcs_to_test:
        rows = func(client)
        print ("\n====%s====" % func.__name__)
        print(rows)

    #bfs(client, 'A', 5)

if __name__ == "__main__":
  main()

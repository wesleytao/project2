import click
from google.cloud import bigquery

uni1 = 'alm2263' # Your uni
uni2 = 'wt2271' # Partner's uni. If you don't have a partner, put None

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
    q = """
        SELECT id, text
        FROM `w4111-columbia.graph.tweets`
        WHERE text LIKE '%going live%' AND text LIKE '%www.twitch%'
        """
        
    job = client.query(q)
    
    results = job.result()
    
    return list(results)

# SQL query for Question 2. You must edit this funtion.
# This function should return a list of days and their corresponding average likes.
def q2(client):
    q = """
        WITH weekday AS (
            SELECT id, like_num, SUBSTR(create_time, 1, 3) AS day
            FROM `w4111-columbia.graph.tweets`
        )
        SELECT day, AVG(like_num) AS avg_likes
        FROM weekday
        GROUP BY day
        ORDER BY AVG(like_num) DESC
        """
        
    job = client.query(q)
    
    results = job.result()
    
    return list(results)

# SQL query for Question 3. You must edit this funtion.
# This function should return a list of source nodes and destination nodes in the graph.
def q3(client):
    q = """
        CREATE OR REPLACE TABLE dataset.GRAPH AS (
            WITH tmp1 AS (
                SELECT id, twitter_username, REGEXP_EXTRACT(text, r"@[^\s']+") AS ex
                FROM `w4111-columbia.graph.tweets`
            )
            SELECT DISTINCT twitter_username as src, SUBSTR(ex, 2) as dst
            FROM tmp1
            WHERE ex IS NOT NULL
        )
        """
    
    job = client.query(q)
    job.result()
    
    
    q2 = """
         SELECT *
         FROM `dataset.GRAPH`
         LIMIT 10
         """
    
    job = client.query(q2)
    
    results = job.result()
    
    return list(results)

# SQL query for Question 4. You must edit this funtion.
# This function should return a list containing the twitter username of the users having the max indegree and max outdegree.
def q4(client):
    q = """
        WITH incoming AS (
            SELECT dst, count(*) AS count
            FROM `dataset.GRAPH`
            GROUP BY dst
            ORDER BY count(*) DESC
            LIMIT 1
        ),
        outgoing AS (
            SELECT src, count(*) AS count
            FROM `dataset.GRAPH`
            GROUP BY src
            ORDER BY count(*) DESC
            LIMIT 1
        )
        SELECT I.dst AS max_indegree, O.src AS max_outdegree
        FROM incoming I, outgoing O
        """
        
    job = client.query(q)
    
    results = job.result()
    
    return list(results)

# SQL query for Question 5. You must edit this funtion.
# This function should return a list containing value of the conditional probability.
def q5(client):
    q = """
        WITH likes AS (
            SELECT twitter_username, AVG(like_num) as avglikes
            FROM `w4111-columbia.graph.tweets`
            GROUP BY twitter_username
        ),
        indegree AS (
            SELECT dst, count(*) AS count
            FROM `dataset.GRAPH`
            GROUP BY dst
        ),
        categorization AS (
            SELECT L.twitter_username AS user, I.count AS indegree, L.avglikes AS likes
            FROM likes L, indegree I
            WHERE L.twitter_username = I.dst
        ),
        popular AS (
            SELECT DISTINCT user
            FROM categorization
            WHERE indegree >= (SELECT AVG(indegree) FROM categorization)
            AND likes >= (SELECT AVG(likes) FROM categorization)
        ),
        unpopular AS (
            SELECT DISTINCT user
            FROM categorization
            WHERE indegree < (SELECT AVG(indegree) FROM categorization)
            AND likes < (SELECT AVG(likes) FROM categorization)           
        ),
        c1 AS (
            SELECT count(user) as count
            FROM unpopular
        ),
        c2 AS (
            SELECT count(user) as count
            FROM unpopular
            WHERE user IN (
                    SELECT U.user
                    FROM unpopular U, `dataset.GRAPH` D
                    WHERE U.user = D.src
                    AND D.dst IN (SELECT * FROM popular)
                    )
        )
        SELECT c2.count/c1.count AS popular_unpopular
        FROM c1, c2
        """
        
    job = client.query(q)
    
    results = job.result()
    
    return list(results)

# SQL query for Question 6. You must edit this funtion.
# This function should return a list containing the value for the number of triangles in the graph.
def q6(client):
    q = """
        WITH triangles AS (
            SELECT DISTINCT G1.src, G2.src, G3.src
            FROM `dataset.GRAPH` G1, `dataset.GRAPH` G2, `dataset.GRAPH` G3
            WHERE G1.dst = G2.src AND G2.dst = G3.src AND G3.dst = G1.dst
        )
        SELECT count(*)
        FROM triangles
        """
        
    job = client.query(q)
    
    results = job.result()
    
    return list(results)

# SQL query for Question 7. You must edit this funtion.
# This function should return a list containing the twitter username and their corresponding PageRank.
def q7(client):
    q1 = """
        CREATE OR REPLACE TABLE dataset.pagerank(twitter_username string, pagerank float64) AS(
            WITH tmp1 AS (
                SELECT src AS twitter_username FROM `dataset.GRAPH`
                UNION DISTINCT
                SELECT dst AS twitter_username FROM `dataset.GRAPH`
            ),
            c AS (
                SELECT count(*) as count
                FROM tmp1
            )
            SELECT tmp1.twitter_username,1/c.count as pagerank
            FROM tmp1, c
        )
         """
    job = client.query(q1)
    job.result()
    
    iterations = 20
    
    for i in range(iterations):
        q2= """
            CREATE OR REPLACE TABLE dataset.tmp(twitter_username string, rank float64) AS (
                WITH outdegree AS (
                    SELECT src, count(*) as count
                    FROM `dataset.GRAPH`
                    GROUP BY src
                ),
                pass AS (
                    SELECT O.src, D.pagerank/O.count as pagerank
                    FROM outdegree O, `dataset.pagerank` D
                    WHERE O.src = D.twitter_username
                ),
                ranks AS (
                    SELECT G.src as src, G.dst as dst, P.pagerank as pagerank
                    FROM `dataset.GRAPH` G, pass P
                    WHERE G.src = P.src
                )
                SELECT dst AS twitter_username, sum(pagerank) as rank
                FROM ranks
                GROUP BY dst
            )
            """
        job = client.query(q2)
        job.result()
        
        q3 = """
            CREATE OR REPLACE TABLE dataset.pagerank(twitter_username string, pagerank float64) AS(
                 SELECT *
                 FROM `dataset.tmp`
            )
            """
            
        job = client.query(q3)
        job.result()

    q4 = """
         SELECT twitter_username, pagerank as page_rank_score
         FROM `dataset.pagerank`
         ORDER BY pagerank DESC
         LIMIT 100
         """
    
    job = client.query(q4)
    results = job.result()
    
    return list(results)
    


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
@click.argument("PATHTOCRED", type=click.Path(exists=True))
def main(pathtocred):
    client = bigquery.Client.from_service_account_json(pathtocred)

    funcs_to_test = [q3]
    #funcs_to_test = [testquery]
    for func in funcs_to_test:
        rows = func(client)
        print ("\n====%s====" % func.__name__)
        print(rows)

    #bfs(client, 'A', 5)

if __name__ == "__main__":
  main()

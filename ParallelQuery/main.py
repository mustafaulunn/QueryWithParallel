import threading
from clickhouse_driver import Client

def run_query(query):
    client = Client('localhost')
    result = client.execute(query)
    print(result)

queries = [
            "SELECT COUNT(*) FROM taxonomy_object_firewalls;",
            "SELECT SUM(Score) FROM taxonomy_object_firewalls",
            "SELECT SUM(Duration) FROM taxonomy_object_firewalls;",
            "SELECT SUM(Duration) FROM taxonomy_object_firewalls WHERE Category='Firewall';",
            "SELECT SUM(Duration) FROM taxonomy_object_firewalls WHERE Category='Firewall' AND AccessGranted='true';",
            "SELECT COUNT(DISTINCT CallerName) FROM taxonomy_object_firewalls;",
            "SELECT COUNT(DISTINCT CallerName) FROM taxonomy_object_firewalls;",
            "SELECT COUNT(DISTINCT Score) FROM taxonomy_object_firewalls;",
            "SELECT Category, COUNT(*) FROM taxonomy_object_firewalls WHERE AccessGranted='true' GROUP BY Category;",
            "SELECT Category, SUM(Duration) FROM taxonomy_object_firewalls GROUP BY Category;",
            "SELECT COUNT(DISTINCT DomainMember) FROM taxonomy_object_firewalls;",
            "SELECT COUNT(DISTINCT Category) FROM taxonomy_object_firewalls;",
            "SELECT COUNT(DISTINCT AccessGranted) FROM taxonomy_object_firewalls;",
            "SELECT COUNT(DISTINCT AccessProperties) FROM taxonomy_object_firewalls;",
            "SELECT COUNT(DISTINCT AccessRequested) FROM taxonomy_object_firewalls;",
            "SELECT COUNT(DISTINCT ExtraField5) FROM taxonomy_object_firewalls;",
            "SELECT COUNT(DISTINCT Action_id) FROM taxonomy_object_firewalls;",
            "SELECT COUNT(*) FROM taxonomy_object_firewalls WHERE AccessGranted = 'True';",
            "SELECT COUNT(*) FROM taxonomy_object_firewalls WHERE AccessGranted = 'False';",
            "SELECT COUNT(*) FROM taxonomy_object_firewalls WHERE ExtraField4 LIKE '%a%';",
            "SELECT COUNT(*) FROM taxonomy_object_firewalls WHERE DSTNAME LIKE '%b%';",
            "SELECT COUNT(*) FROM taxonomy_object_firewalls WHERE Duration > 300;",
            "SELECT COUNT(*) from taxonomy_object_firewalls WHERE Database_principal_id>30;",
            "SELECT SUM(AlexaIndex) from taxonomy_object_firewalls WHERE AlexaIndex>100;"


 ]

threads = []
for query in queries:
    t = threading.Thread(target=run_query, args=(query,))
    threads.append(t)
    t.start()

# Wait for all threads to finish
for t in threads:
    t.join()

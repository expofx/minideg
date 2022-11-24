"""
Turn a grammar.json into neo4j graph.
"""

import sys
sys.path.append("..")
from deg.core import hypergraph
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
import nxneo4j as nx

class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.G = nx.Graph(self.driver)

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()
        
    def create_graph(self, hg, type):
        self.G.add_nodes_from(hg.nodes, type=type) # list [node1, node2, node3]
        self.G.add_edges_from(hg.edges, type=type) # tuple of lists (node1, node2)
        
    def create_atom(self, atom):
        with self.driver.session(database="neo4j") as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_atom, atom)
            for row in result:
                print("Created atom: {a}".format(a=row['a']))
    
    @staticmethod
    def _create_atom(self, tx, atom):
        result = tx.run("CREATE (a:Atom {name: $name}) "
                        "RETURN a.name AS a", name=atom)
        return [{"a": record["a"]} for record in result]

    def create_bond(self, atom1, atom2):
        with self.driver.session(database="neo4j") as session:
            # Write transactions allow the driver to handle retries and transient errors
            result = session.write_transaction(
                self._create_bond, atom1, atom2)
            for row in result:
                print("Created bond between: {a1}, {a2}".format(a1=row['a1'], a2=row['a2']))

    @staticmethod
    def _create_bond(tx, atom1_name, atom2_name):
        # To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        # The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        query = (
            "CREATE (a1:Atom { name: $atom1_name }) "
            "CREATE (a2:Atom { name: $atom2_name }) "
            "CREATE (a1)-[:BONDED_WITH]->(a2) "
            "RETURN a1, a2"
        )
        result = tx.run(query, atom1_name=atom1_name, atom2_name=atom2_name)
        try:
            return [{"a1": row["a1"]["name"], "a2": row["a2"]["name"]}
                    for row in result]
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise


if __name__ == "__main__":
    # Aura queries use an encrypted connection using the "neo4j+s" URI scheme
    uri = input("Enter your Aura database URI: ")
    user = input("Enter your database user name: ")
    password = input("Enter your database password: ")
    app = App(uri, user, password)
    # app.create_bond("C", "O")

    # grammar.json

    import json
    from copy import deepcopy
    from rdkit import Chem
    import sys; sys.path.append("..")
    from deg.core import Hypergraph, ProductionRuleCorpus, ProductionRule, rule_to_mol
    from pydantic import BaseModel, Field, PrivateAttr

    # grammar.json back into ProductionRuleCorpus

    path = "../examples/grammar.json"
    with open(path, 'r') as f:
        raw = json.loads(f.read())

    grammar = ProductionRuleCorpus(**raw)
    rules = grammar.prod_rule_list
    
    # iterate through prod rules and create graph
    
    for rule in rules:
        # create hypergraph
        lhs = rule.lhs
        rhs = rule.rhs
        # create graph
        app.create_graph(lhs, type="lhs")
        app.create_graph(rhs, type="rhs")
        app.create_bond(lhs, rhs)

    app.close()
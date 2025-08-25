"""
Dgraph DAO implementation - imports from modular structure.

This file maintains backward compatibility by re-exporting the DgraphDAO
class from its new modular location.
"""

from .graph.dgraph_dao import DgraphDAO

__all__ = ["DgraphDAO"]
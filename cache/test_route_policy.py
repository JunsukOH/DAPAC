import importlib.util
from pathlib import Path
import unittest

spec=importlib.util.spec_from_file_location('route_policy',Path(__file__).with_name('ontime-cache-route.py'))
policy=importlib.util.module_from_spec(spec);spec.loader.exec_module(policy)
RULE={'priority':1053,'src':'192.0.2.20','dst':'198.51.100.10','table':1053}
ROUTE={'dst':'198.51.100.10','gateway':'192.0.2.17','dev':'eth0','prefsrc':'192.0.2.20'}


class Tests(unittest.TestCase):
    def test_missing_rule_with_retained_route_restores_only_rule(self):
        actions=policy.plan([], [ROUTE])
        self.assertEqual(len(actions),1);self.assertEqual(actions[0][2:4],['rule','add'])

    def test_missing_route_with_retained_rule_restores_only_route(self):
        actions=policy.plan([RULE],[])
        self.assertEqual(len(actions),1);self.assertEqual(actions[0][2:4],['route','add'])

    def test_complete_policy_is_idempotent(self):self.assertEqual(policy.plan([RULE],[ROUTE]),[])

    def test_empty_state_restores_route_then_rule(self):
        actions=policy.plan([],[])
        self.assertEqual([a[2] for a in actions],['route','rule'])

    def test_foreign_rule_is_preserved(self):
        with self.assertRaises(ValueError):policy.plan([dict(RULE,src='192.0.2.1')],[ROUTE])

    def test_foreign_route_is_preserved(self):
        with self.assertRaises(ValueError):policy.plan([], [dict(ROUTE,gateway='192.0.2.1')])


if __name__=='__main__':unittest.main()

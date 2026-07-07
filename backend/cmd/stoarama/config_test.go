package main

import "testing"

func TestLegacyNodeMigratesToNodesOnly(t *testing.T) {
	cfg := cliConfig{
		Node: &cliNodeConfig{ID: 7, NodeType: "local_recorder", Token: "tok"},
	}
	if cfg.Nodes == nil {
		cfg.Nodes = map[string]*cliNodeConfig{}
	}
	if cfg.Node != nil {
		cp := *cfg.Node
		cfg.Nodes[cp.NodeType] = &cp
		cfg.Node = nil
	}
	node := nodeConfigForType(cfg, "local_recorder")
	if node == nil || node.ID != 7 || node.Token != "tok" {
		t.Fatalf("node=%+v", node)
	}
	setNodeConfig(&cfg, &cliNodeConfig{ID: 8, NodeType: "inference_node", Token: "tok2"})
	if cfg.Node != nil {
		t.Fatalf("legacy node write path should stay nil")
	}
	if got := nodeConfigForType(cfg, "inference_node"); got == nil || got.ID != 8 {
		t.Fatalf("inference node=%+v", got)
	}
}

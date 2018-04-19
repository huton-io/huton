package huton

import "testing"

func TestInstance_Peers(t *testing.T) {
	instance := &Instance{
		peers: map[string]*Peer{
			"test1": {
				Name: "test1",
			},
			"test2": {
				Name: "test2",
			},
		},
	}
	peers := instance.Peers()
	if len(peers) != len(instance.peers) {
		t.Errorf("Number of returned peers does not match the number of peers in the instance: %d != %d", len(peers), len(instance.peers))
	}
}

func TestInstance_Local(t *testing.T) {
	instance := &Instance{
		name: "test1",
		peers: map[string]*Peer{
			"test1": {
				Name: "test1",
			},
			"test2": {
				Name: "test2",
			},
		},
	}
	local := instance.Local()
	if local == nil {
		t.Error("Could not find local peer in instance peer list.")
	}
}

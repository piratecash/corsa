package node

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"reflect"
	"strings"
	"testing"
)

// Closed AST node/site multisets, including declarations/signatures/GenDecls.
// Expected sites are literal reviewed inputs, never learned from current code.
func ninN1SourceErrors(sources map[string][]byte) []string {
	expected := map[string]map[string]int{
		"runWithFiles": {
			"identity_pin_inbound_lifecycle.go:run": 1,
		},
		"selectInboundSend": {
			"network_bridge.go:sendFrameBound": 1,
		},
		"selectInboundClose": {
			"network_bridge.go:Close": 1,
		},
		"inboundSendSelection": {
			"network_bridge.go:<package>":           1,
			"network_bridge.go:selectInboundSend":   2,
			"network_bridge.go:sendCapturedInbound": 1,
		},
		"inboundCloseSelection": {
			"network_bridge.go:<package>":            1,
			"network_bridge.go:closeCapturedInbound": 1,
			"network_bridge.go:selectInboundClose":   2,
		},
		"bindInboundLeafLocked": {
			"conn_registry.go:bindProofTransportLocked": 1,
		},
		"inboundTerminalForWire": {
			"service.go:handleConn": 1,
		},
		"transportTerminalLocked": {
			"conn_registry.go:condemnInboundSelected":      1,
			"conn_registry.go:inboundTerminalForWire":      1,
			"conn_registry.go:staleInboundTerminalsLocked": 1,
			"network_bridge.go:selectInboundClose":         1,
			"network_bridge.go:selectInboundSend":          1,
			"service.go:enqueueFrameByID":                  1,
			"service.go:enqueueFrameSyncByID":              1,
		},
		"selectInboundPinAfterLocked": {
			"identity_pin_inbound_lifecycle.go:attempt": 1,
		},
		"endInboundLeavesLocked": {
			"service.go:closeAllInboundConns": 1,
		},
		"staleInboundTerminalsLocked": {
			"peer_management.go:evictStaleInboundConns": 1,
		},
		"condemnInboundSelected": {
			"loopback_inbound.go:chargeLoopbackViolation": 2,
		},
		"inboundPinProducer": {
			"identity_pin_inbound.go:<package>":                           1,
			"identity_pin_inbound_lifecycle.go:<package>":                 1,
			"identity_pin_inbound_lifecycle.go:attempt":                   1,
			"identity_pin_inbound_lifecycle.go:attachInboundPinCandidate": 1,
			"identity_pin_inbound_lifecycle.go:notify":                    1,
			"identity_pin_inbound_lifecycle.go:pass":                      1,
			"identity_pin_inbound_lifecycle.go:run":                       1,
			"identity_pin_inbound_lifecycle.go:runWithFiles":              1,
			"service.go:<package>":                                        1,
		},
		"inboundPins": {
			"conn_registry.go:bindInboundLeafLocked":                      3,
			"conn_registry.go:publishInboundLifetime":                     5,
			"conn_registry.go:selectInboundPinAfterLocked":                1,
			"identity_pin_inbound_lifecycle.go:attachInboundPinCandidate": 2,
			"network_consumer.go:sendInboundBound":                        1,
			"service.go:<package>":                                        1,
			"service.go:Run":                                              2,
			"session_secure.go:acceptSecureInbound":                       3,
		},
		"inboundProvisional": {
			"conn_registry.go:bindInboundLeafLocked":      1,
			"conn_registry.go:publishInboundLifetime":     1,
			"identity_pin_inbound_lifecycle.go:<package>": 1,
		},
		"inboundPublished": {
			"conn_registry.go:publishInboundLifetime":      1,
			"conn_registry.go:selectInboundPinAfterLocked": 1,
			"identity_pin_inbound.go:stageInboundPin":      1,
			"identity_pin_inbound_lifecycle.go:<package>":  1,
		},
		"inboundTerminal": {
			"identity_pin_inbound_lifecycle.go:<package>": 1,
			"proof_transport_lifetime.go:end":             1,
		},
		"newInboundPinCandidateService": {
			"find_node_connection.go:newFindNodeInboundCandidateService": 1,
		},
		"newUnknownInboundPinCandidateService": {},
		"attachInboundPinCandidate": {
			"identity_pin_inbound_lifecycle.go:newInboundPinCandidateService":        1,
			"identity_pin_inbound_lifecycle.go:newUnknownInboundPinCandidateService": 1,
		},
		"installUnknownInboundPinJournal": {
			"identity_pin_inbound_lifecycle.go:newUnknownInboundPinCandidateService": 1,
		},
		"ninInstallResult": {
			"identity_pin_inbound_install.go:<package>":                              1,
			"identity_pin_inbound_install.go:installUnknownInboundPinJournal":        2,
			"identity_pin_inbound_lifecycle.go:newUnknownInboundPinCandidateService": 2,
			"identity_pin_inbound_lifecycle.go:attachInboundPinCandidate":            1,
		},
		"stageInboundPin": {
			"identity_pin_inbound_lifecycle.go:attempt": 1,
		},
		"publishInboundLifetime": {
			"conn_registry.go:publishAcceptedProofTransport": 1,
		},
		"acceptedTransportArrivalContext": {
			"datagram_integration.go:dispatchInboundDatagramLineOwned": 1,
		},
		"transportArrivalOwner": {
			"identity_discovery.go:Handle": 1,
		},
		"newServiceWithDatagramConstructor": {
			"find_node_degree.go:newDegreeService":                                   1,
			"find_node_connection.go:newFindNodeCandidateService":                    1,
			"identity_pin_inbound_lifecycle.go:newInboundPinCandidateService":        1,
			"identity_pin_inbound_lifecycle.go:newUnknownInboundPinCandidateService": 1,
			"service.go:newService":                                                  1,
		},
		"inboundPinClock": {
			"service.go:newServiceWithDatagramConstructor":                           1,
			"find_node_connection.go:newFindNodeInboundCandidateService":             1,
			"identity_pin_inbound_lifecycle.go:<package>":                            1,
			"identity_pin_inbound_lifecycle.go:newInboundPinCandidateService":        1,
			"identity_pin_inbound_lifecycle.go:newUnknownInboundPinCandidateService": 1,
			"service.go:newServiceWithConstruction":                                  1,
		},
		"proofTransportArrivalKey": {
			"identity_pin_inbound_lifecycle.go:<package>":                       1,
			"identity_pin_inbound_lifecycle.go:acceptedTransportArrivalContext": 1,
			"identity_pin_inbound_lifecycle.go:sessionTransportArrivalContext":  1,
			"identity_pin_inbound_lifecycle.go:transportArrivalOwner":           1,
		},
		"matureInboundWitness": {
			"conn_registry.go:selectInboundPinAfterLocked": 2,
			"identity_pin_inbound.go:<package>":            1,
			"identity_pin_inbound.go:stageInboundPin":      1,
			"identity_pin_inbound_lifecycle.go:attempt":    1,
			"proof_transport_lifetime.go:<package>":        1,
			"session_secure.go:acceptSecureInbound":        1,
		},
		"proofTransportOwner": {
			"identity_record_admission.go:prepareContactRecordPush":             1,
			"conn_registry.go:<package>":                                        1,
			"conn_registry.go:inboundTerminalForWire":                           1,
			"conn_registry.go:publishAcceptedProofTransport":                    1,
			"conn_registry.go:publishInboundLifetime":                           1,
			"conn_registry.go:transportTerminalLocked":                          1,
			"datagram_integration.go:dispatchInboundDatagramLineOwned":          1,
			"datagram_integration.go:dispatchInboundDatagramWireOwned":          1,
			"identity_discovery.go:closePushSessionOwned":                       1,
			"identity_discovery.go:sendInitialIdentityPushOwned":                1,
			"identity_pin_inbound_lifecycle.go:acceptedTransportArrivalContext": 1,
			"identity_pin_inbound_lifecycle.go:captureSessionTransportOwner":    1,
			"identity_pin_inbound_lifecycle.go:transportArrivalOwner":           2,
			"network_bridge.go:<package>":                                       1,
			"network_bridge.go:sendCapturedInbound":                             1,
			"network_bridge.go:sendFrameBound":                                  1,
			"network_consumer.go:sendInboundBound":                              1,
			"peer_management.go:inboundHeartbeatOwned":                          1,
			"proof_transport_lifetime.go:<package>":                             1,
			"proof_transport_lifetime.go:end":                                   1,
			"proof_transport_lifetime.go:matchesTransportLocked":                1,
			"proof_transport_lifetime.go:ownerLocked":                           2,
			"service.go:dispatchNetworkFrameOwned":                              1,
			"service.go:handleAckDeleteFrameOwned":                              1,
			"service.go:handleAuthSessionOwned":                                 1,
			"service.go:handleCommandOwned":                                     1,
			"service.go:handleConn":                                             1,
			"service.go:readInboundCommandLineOwned":                            1,
			"service.go:refuseOversizeInboundLineOwned":                         1,
			"service.go:startInboundSessionDeliveryOwned":                       1,
			"session_secure.go:acceptProvenInboundOwned":                        1,
		},
		"proofTransportLifetime": {
			"find_node_degree.go:<package>":                 2,
			"find_node_degree.go:publishDegreeTransport":    1,
			"find_node_degree.go:degreeEndLocked":           1,
			"find_node_connection.go:<package>":             1,
			"find_node_connection.go:captureFindNodeSeed":   1,
			"find_node_connection.go:publishFindNodeLocked": 1,
			"identity_pin_inbound.go:<package>":             1,
			"net_core.go:<package>":                         1,
			"proof_transport_lifetime.go:<package>":         2,
			"proof_transport_lifetime.go:end":               1,
			"proof_transport_lifetime.go:ownerLocked":       1,
			"service.go:<package>":                          1,
			"session_secure.go:<package>":                   2,
			"session_secure.go:acceptSecureInbound":         1,
			"session_secure.go:bindProofTransport":          1,
			"session_secure.go:provenTransport":             1,
		},
		"proofTransportServiceToken": {
			"find_node_degree.go:<package>":         1,
			"proof_transport_lifetime.go:<package>": 3,
			"service.go:<package>":                  1,
			"service.go:newServiceWithConstruction": 1,
		},
		"proofTransportEvidence": {
			"proof_transport_lifetime.go:<package>": 1,
			"session_secure.go:<package>":           1,
			"session_secure.go:acceptSecureInbound": 1,
			"session_secure.go:provenTransport":     1,
		},
		"proofToken": {
			"find_node_degree.go:newDegreeService":                           1,
			"find_node_degree.go:publishDegreeTransport":                     2,
			"conn_registry.go:bindProofTransportLocked":                      1,
			"conn_registry.go:transportTerminalLocked":                       1,
			"find_node_connection.go:publishFindNodeCM":                      1,
			"identity_pin_inbound_lifecycle.go:captureSessionTransportOwner": 1,
			"proof_transport_lifetime.go:matchesTransportLocked":             1,
			"service.go:<package>":                                           1,
			"service.go:newServiceWithConstruction":                          1,
			"session_secure.go:acceptSecureInbound":                          1,
			"session_secure.go:provenTransport":                              1,
			"find_node_connection.go:findNodePriorityMemberLocked":           1,
			"find_node_connection.go:findNodePrioritySnapshot":               1,
		},
		"findNode": {
			"conn_registry.go:publishAcceptedProofTransport":             2,
			"find_node_connection.go:captureFindNodeSeed":                2,
			"find_node_connection.go:findNodeMembers":                    3,
			"find_node_connection.go:newFindNodeCandidateService":        1,
			"find_node_connection.go:newFindNodeInboundCandidateService": 1,
			"find_node_connection.go:publishFindNodeCM":                  2,
			"find_node_connection.go:publishFindNodeLocked":              4,
			"network_consumer.go:sendInboundBound":                       1,
			"proof_transport_lifetime.go:<package>":                      1,
			"proof_transport_lifetime.go:end":                            2,
			"service.go:<package>":                                       1,
			"session_secure.go:acceptSecureInbound":                      1,
			"session_secure.go:dialV2":                                   1,
			"session_secure.go:provenTransport":                          1,
			"find_node_connection.go:findNodePriorityMemberLocked":       1,
			"find_node_connection.go:findNodePrioritySnapshot":           3,
		},
		"findNodeAdapter": {
			"find_node_connection.go:<package>":                          1,
			"find_node_connection.go:newFindNodeCandidateService":        1,
			"find_node_connection.go:newFindNodeInboundCandidateService": 1,
			"service.go:<package>":                                       1,
		},
		"findNodeConnectionBinding": {
			"find_node_connection.go:<package>":           1,
			"find_node_connection.go:captureFindNodeSeed": 1,
			"find_node_connection.go:end":                 1,
			"proof_transport_lifetime.go:<package>":       1,
		},
		"findNodeCMReceipt": {
			"find_node_connection.go:<package>":                       1,
			"find_node_connection.go:prepareFindNodeCMAcceptedLocked": 2,
			"service.go:<package>":                                    1,
		},
		"newFindNodeCandidateService":        {},
		"newFindNodeInboundCandidateService": {},
		"captureFindNodeSeed": {
			"session_secure.go:acceptSecureInbound": 1,
			"session_secure.go:provenTransport":     1,
		},
		"publishFindNodeLocked": {
			"conn_registry.go:publishAcceptedProofTransport": 1,
			"find_node_connection.go:publishFindNodeCM":      1,
		},
		"publishAcceptedProofTransport": {
			"service.go:handleConn": 1,
		},
		"prepareFindNodeCMAcceptedLocked": {
			"peer_sessions.go:onCMSessionEstablished": 1,
		},
		"publishFindNodeCM": {
			"peer_sessions.go:publishAndServeCMSession": 1,
		},
		"findNodeMembers": {},
		"bindProofTransportLocked": {
			"conn_registry.go:attachOutboundCoreLocked":  1,
			"conn_registry.go:registerInboundConnLocked": 1,
		},
		"bindProofTransport": {
			"session_secure.go:acceptSecureInbound": 2,
			"session_secure.go:provenTransport":     2,
		},
		"endProofTransportIO": {
			"session_secure.go:Close": 1,
			"session_secure.go:Read":  1,
			"session_secure.go:Write": 1,
		},
		"endProofTransport": {
			"connection_manager.go:deactivateSlotLocked":            1,
			"connection_manager.go:handleDialSucceeded":             1,
			"peer_management.go:readPeerSession":                    5,
			"peer_session_admission.go:punishSessionAdmission":      1,
			"peer_session_admission.go:refuseOversizeContactsReply": 1,
			"peer_sessions.go:servePeerSession":                     9,
			"service.go:closeAs":                                    1,
		},
		"ownerLocked": {
			"conn_registry.go:transportTerminalLocked":                       1,
			"identity_pin_inbound_lifecycle.go:captureSessionTransportOwner": 1,
		},
		"matchesLocked": {
			"identity_pin_contact_record.go:prepareContactRecordStageLocked":      1,
			"identity_pin_contact_record.go:matchesLocked":                        1,
			"identity_pin_contact.go:stageContactPin":                             1,
			"identity_pin_contact_record.go:checkLocked":                          1,
			"identity_pin_contact_record.go:mirrorContactRecordOwner":             1,
			"identity_pin_contact_record.go:stageContactRecordLocked":             1,
			"identity_pin_inbound.go:stageInboundPin":                             1,
			"identity_pin_outgoing_provenance.go:claimR2aFirstDirectedItemLocked": 2,
			"identity_pin_outgoing_provenance.go:registerR2aOrigin":               1,
			"identity_pin_outgoing_provenance.go:settleR2aOwnHopLocked":           2,
			"identity_pin_relation.go:matchesLocked":                              1,
			"identity_pin_relation.go:pinOwnerObservationLocked":                  1,
			"identity_pin_relation.go:stageRelationRecordLocked":                  1,
			"identity_pin_relation.go:stageClassifiedRelationRecordLocked":        1,
			"identity_record_admission_own_dm.go:ownersMatchLocked":               2,
			"identity_pin_relation_completion.go:checkLocked":                     1,
			"identity_pin_selected.go:selectedPinStatus":                          1,
			"identity_pin_selected.go:stageSelectedPin":                           1,
			"identity_record_admission_relation.go:stageRelationAdmission":        1,
			"v2_identity_claims.go:current":                                       1,
		},
		"captureSessionTransportOwner": {
			"identity_pin_inbound_lifecycle.go:sessionTransportArrivalContext": 1,
			"peer_sessions.go:completeOutboundAuth":                            1,
		},
		"sessionTransportArrivalContext": {
			"datagram_integration.go:dispatchSessionDatagramLine": 1,
		},
		"findNodeMemberSnapshot": {
			"find_node_connection.go:<package>":               1,
			"find_node_connection.go:findNodeMembers":         4,
			"find_node_connection.go:findNodePriorityMembers": 6,
		},
		"findnode": {
			"find_node_connection.go:<package>":                      4,
			"find_node_connection.go:captureFindNodeSeed":            1,
			"find_node_connection.go:end":                            1,
			"find_node_connection.go:publishFindNodeLocked":          2,
			"find_node_connection.go:findNodePriorityMemberLocked":   10,
			"find_node_connection.go:findNodePriorityMembers":        6,
			"find_node_connection.go:captureFindNodePriorityClasses": 8,
			"find_node_connection.go:findNodePrioritySnapshot":       20,
		},
		"PrepareSession": {
			"find_node_connection.go:captureFindNodeSeed": 1,
		},
		"Bind": {
			"find_node_connection.go:publishFindNodeLocked": 1,
		},
		"End": {
			"find_node_connection.go:end": 1,
		},
		"NewSession": {},
		"matchesTransportLocked": {
			"identity_record_admission.go:prepareContactRecordPush":             1,
			"conn_registry.go:publishAcceptedProofTransport":                    1,
			"identity_pin_inbound_lifecycle.go:acceptedTransportArrivalContext": 1,
			"identity_pin_inbound_lifecycle.go:transportArrivalOwner":           1,
		},
	}
	expectedEnds := map[string]int{
		"conn_registry.go:condemnInboundSelected":                  1,
		"conn_registry.go:endInboundLeavesLocked":                  1,
		"conn_registry.go:publishAcceptedProofTransport":           1,
		"conn_registry.go:staleInboundTerminalsLocked":             1,
		"conn_registry.go:unregisterConnLocked":                    1,
		"datagram_integration.go:dispatchInboundDatagramWireOwned": 1,
		"identity_discovery.go:Handle":                             1,
		"identity_discovery.go:closePushSessionOwned":              1,
		"identity_discovery.go:sendInitialIdentityPushOwned":       1,
		"network_bridge.go:selectInboundClose":                     1,
		"network_consumer.go:sendFrameBytesViaNetwork":             1,
		"network_consumer.go:sendFrameBytesViaNetworkSync":         1,
		"network_consumer.go:sendFrameViaNetwork":                  1,
		"network_consumer.go:sendFrameViaNetworkSync":              1,
		"peer_management.go:inboundHeartbeatOwned":                 1,
		"proof_transport_lifetime.go:end":                          2,
		"proof_transport_lifetime.go:endProofTransport":            1,
		"service.go:dispatchNetworkFrameOwned":                     9,
		"service.go:enqueueFrameByID":                              1,
		"service.go:enqueueFrameSyncByID":                          2,
		"service.go:handleAckDeleteFrameOwned":                     5,
		"service.go:handleAuthSessionOwned":                        2,
		"service.go:handleCommandOwned":                            1,
		"service.go:handleConn":                                    6,
		"service.go:readInboundCommandLineOwned":                   1,
		"service.go:refuseOversizeInboundLineOwned":                3,
		"session_secure.go:acceptProvenInboundOwned":               2,
		"session_secure.go:acceptSecureInbound":                    1,
		"session_secure.go:bindProofTransport":                     1,
		"session_secure.go:endProofTransportIO":                    1,
		"session_secure.go:provenTransport":                        1,
	}
	constructors := map[string]map[string]int{
		"inboundSendSelection": {
			"network_bridge.go:selectInboundSend": 1,
		},
		"inboundCloseSelection": {
			"network_bridge.go:selectInboundClose": 1,
		},
		"matureInboundWitness": {
			"session_secure.go:acceptSecureInbound": 1,
		},
		"proofTransportOwner": {
			"proof_transport_lifetime.go:ownerLocked": 1,
		},
		"inboundPinProducer": {
			"identity_pin_inbound_lifecycle.go:attachInboundPinCandidate": 1,
		},
		"proofTransportArrivalKey": {
			"identity_pin_inbound_lifecycle.go:acceptedTransportArrivalContext": 1,
			"identity_pin_inbound_lifecycle.go:sessionTransportArrivalContext":  1,
			"identity_pin_inbound_lifecycle.go:transportArrivalOwner":           1,
		},
		"inboundPinClock": {},
		"proofTransportLifetime": {
			"session_secure.go:acceptSecureInbound": 1,
			"session_secure.go:provenTransport":     1,
		},
		"proofTransportServiceToken": {
			"service.go:newServiceWithConstruction": 1,
		},
		"proofTransportEvidence": {
			"session_secure.go:acceptSecureInbound": 1,
			"session_secure.go:provenTransport":     1,
		},
		"findNodeAdapter": {
			"find_node_connection.go:newFindNodeCandidateService":        1,
			"find_node_connection.go:newFindNodeInboundCandidateService": 1,
		},
		"findNodeConnectionBinding": {
			"find_node_connection.go:captureFindNodeSeed": 1,
		},
		"findNodeCMReceipt": {
			"find_node_connection.go:prepareFindNodeCMAcceptedLocked": 1,
		},
	}
	// Constructor body relocation plus exact immutable terminal additions.
	expected["inboundPinClock"]["service.go:newServiceWithRecordAdmissionClasses"] = 1
	expected["proofTransportLifetime"]["find_node_degree_terminal_bridge.go:endBridgeLocked"] = 1
	expected["proofTransportLifetime"]["find_node_degree_terminal_bridge.go:clearBridgeTerminalLocked"] = 1
	expected["proofTransportServiceToken"]["find_node_bridge_construction.go:<package>"] = 1
	expected["proofTransportServiceToken"]["find_node_bridge_construction.go:newFindNodeTerminalConstruction"] = 1
	expected["proofTransportServiceToken"]["find_node_degree_terminal_bridge.go:<package>"] = 1
	expected["proofToken"]["service.go:newServiceWithConstruction"] = 4
	expected["findNode"]["find_node_degree_terminal_bridge.go:clearBridgeTerminalLocked"] = 2
	expected["inboundTerminal"]["find_node_degree_terminal_bridge.go:clearBridgeTerminalLocked"] = 1
	constructors["proofTransportServiceToken"]["find_node_bridge_construction.go:newFindNodeTerminalConstruction"] = 1
	expectedEnds["find_node_degree_terminal_bridge.go:clearBridgeTerminalLocked"] = 1
	// S1: immutable optional same-domain F1 adapter and one owned open branch.
	expected["proofToken"]["find_node_connection.go:publishFindNodeLocked"] = 1
	expected["findNode"]["find_node_connection.go:publishFindNodeLocked"] = 5
	expected["findNode"]["find_node_connection.go:findNodePrioritySnapshot"] = 5
	expected["findNode"]["find_node_bridge_construction.go:<package>"] = 1
	expected["findNode"]["find_node_bridge_construction.go:newFindNodeOwnedF1CandidateService"] = 1
	expected["findNode"]["find_node_bridge_construction.go:claim"] = 4
	expected["findNode"]["service.go:newServiceWithConstruction"] = 5
	expected["findNodeAdapter"]["find_node_bridge_construction.go:<package>"] = 1
	expected["findNodeAdapter"]["find_node_bridge_construction.go:newFindNodeOwnedF1CandidateService"] = 1
	expected["findNodeAdapter"]["service.go:newServiceWithConstruction"] = 1
	expected["findnode"]["find_node_connection.go:publishFindNodeLocked"] = 4
	expected["findnode"]["find_node_connection.go:findNodePrioritySnapshot"] = 22
	expected["Bind"]["find_node_connection.go:publishFindNodeLocked"] = 2
	constructors["findNodeAdapter"]["find_node_bridge_construction.go:newFindNodeOwnedF1CandidateService"] = 1
	// S3A: private unused raw originals/classes; no new publication or caller.
	expected["proofToken"]["find_node_connection.go:collectOwnedFindNodeRawFacts"] = 1
	expected["proofToken"]["find_node_connection.go:collectOwnedFindNodeRawMemberLocked"] = 1
	expected["findNode"]["find_node_connection.go:collectOwnedFindNodeRawFacts"] = 3
	expected["findNode"]["find_node_connection.go:collectOwnedFindNodeRawMemberLocked"] = 1
	expected["findnode"]["find_node_connection.go:<package>"] = 6
	expected["findnode"]["find_node_connection.go:captureFindNodePriorityClasses"] = 2
	expected["findnode"]["find_node_connection.go:findNodePriorityClassLocked"] = 7
	expected["findnode"]["find_node_connection.go:collectOwnedFindNodeRawFacts"] = 8
	expected["findnode"]["find_node_connection.go:collectOwnedFindNodeRawMemberLocked"] = 13
	expected["findnode"]["find_node_connection.go:collectOwnedFindNodeRawClasses"] = 4
	// S3B2a: exact new original Node observation proof/adapter references.
	expected["proofToken"]["find_node_bridge_capture.go:captureOwnedFindNodeObservation"] = 1
	expected["proofToken"]["find_node_degree_bridge.go:captureBridgeDegreeProjection"] = 1
	expected["findNode"]["find_node_bridge_construction.go:prepareFindNodeOwnedCaptureConstruction"] = 1
	expected["findNode"]["find_node_bridge_capture.go:captureOwnedFindNodeObservation"] = 3
	expected["findNode"]["find_node_bridge_construction.go:claim"] = 5
	expected["findNodeAdapter"]["find_node_bridge_construction.go:prepareFindNodeOwnedCaptureConstruction"] = 1
	expected["findnode"]["find_node_bridge_capture.go:captureOwnedFindNodeObservation"] = 4
	constructors["findNodeAdapter"]["find_node_bridge_construction.go:prepareFindNodeOwnedCaptureConstruction"] = 1
	// S3B2b: exact dormant pair and sole complete opaque-cut conversion.
	expected["findnode"]["find_node_bridge_construction.go:<package>"] = 1
	expected["findnode"]["find_node_bridge_construction.go:prepareFindNodeBridgeConstruction"] = 2
	expected["findnode"]["find_node_bridge_construction.go:claim"] = 2
	expected["findnode"]["find_node_bridge_capture.go:<package>"] = 2
	expected["findnode"]["find_node_bridge_capture.go:captureOwnedFindNodeCut"] = 10

	// C3d: original bounded receive lanes and dormant known-target owned offer.
	expected["proofTransportOwner"]["find_node_owned_receive.go:<package>"] = 2
	expected["proofTransportOwner"]["find_node_owned_receive.go:recordOwnedFindNodeReceive"] = 1
	expected["proofTransportOwner"]["find_node_owned_receive.go:recordOwnedFindNodeLine"] = 1
	expected["proofTransportOwner"]["find_node_owned_receive.go:readInboundOwnedFindNodeLine"] = 1
	expected["proofTransportOwner"]["find_node_owned_receive.go:readOutboundOwnedFindNodeLine"] = 1
	expected["proofTransportOwner"]["find_node_owned_receive.go:readOutboundOwnedFindNodeBeforeCharge"] = 1
	expected["proofTransportOwner"]["peer_management.go:readPeerSession"] = 1
	expected["ownerLocked"]["peer_management.go:readPeerSession"]++
	expected["proofToken"]["find_node_owned_receive.go:recordOwnedFindNodeReceive"] = 1
	expected["proofToken"]["find_node_owned_receive.go:takeOwnedFindNodeReceive"] = 1
	expected["findnode"]["find_node_owned_receive.go:<package>"] = 1
	expected["findnode"]["find_node_owned_receive.go:recordOwnedFindNodeReceive"] = 3
	expected["findnode"]["find_node_owned_receive.go:takeOwnedFindNodeReceive"] = 5
	expected["findnode"]["find_node_owned_receive.go:recordOwnedFindNodeLine"] = 2
	expected["findnode"]["find_node_owned_receive.go:dispatchOwnedFindNodeReceive"] = 5
	expected["findnode"]["peer_management.go:readPeerSession"] = 1
	expected["findnode"]["find_node_bridge_capture.go:captureOwnedFindNodeCut"] = 2
	expected["findnode"]["find_node_bridge_capture.go:sealOwnedFindNodeObservation"] = 9
	// C3e: original scheduler and writer custody, before Ready.
	expected["findnode"]["find_node_connection.go:<package>"] += 2
	expected["findnode"]["proof_transport_lifetime.go:end"] = 4
	expected["findnode"]["find_node_owned_round_loop.go:prepareFindNodeOwnedRoundConstruction"] = 1
	expected["findnode"]["find_node_owned_write.go:queueOwnedFindNodeForward"] = 4
	expected["findNode"]["find_node_connection.go:captureFindNodeSeed"] += 3
	expected["findNode"]["proof_transport_lifetime.go:end"] += 3
	expected["findNode"]["find_node_owned_round_loop.go:prepareFindNodeOwnedRoundConstruction"] = 1
	expected["findNode"]["find_node_owned_write.go:queueOwnedFindNodeForward"] = 3
	expected["proofToken"]["find_node_owned_write.go:queueOwnedFindNodeForward"] = 1
	expected["findnode"]["find_node_owned_answer.go:dispatchOwnedFindNodeAnswer"] = 4
	expected["findnode"]["find_node_owned_answer.go:queueOwnedFindNodeAnswer"] = 4
	expected["findnode"]["find_node_owned_answer.go:dispatchOutboundOwnedFindNode"] = 2
	expected["findNode"]["find_node_owned_answer.go:queueOwnedFindNodeAnswer"] = 3
	expected["proofToken"]["find_node_owned_answer.go:dispatchOwnedFindNodeAnswer"] = 1
	expected["proofToken"]["find_node_owned_answer.go:queueOwnedFindNodeAnswer"] = 1

	expected["findnode"]["find_node_resources.go:<package>"] = 2
	expected["findnode"]["find_node_owned_round_loop.go:<package>"] = 1
	// Local target original custody, before Service/F1 escape; forwarding retained.
	expected["findnode"]["find_node_connection.go:<package>"] += 2
	expected["findnode"]["find_node_bridge_capture.go:<package>"] += 1
	expected["findnode"]["proof_transport_lifetime.go:end"] += 3
	expected["findNode"]["find_node_connection.go:captureFindNodeSeed"] += 2
	expected["findNode"]["proof_transport_lifetime.go:end"] += 1
	expected["findNode"]["find_node_owned_local_target.go:prepareFindNodeOwnedLocalTargetConstruction"] = 1
	expected["findnode"]["find_node_owned_local_target.go:<package>"] = 1
	expected["findnode"]["find_node_owned_local_target.go:newOwnedFindNodeLocalLoop"] = 1
	expected["findnode"]["find_node_owned_local_target.go:newFindNodeOwnedLocalTargetCandidateService"] = 1
	expected["findnode"]["find_node_owned_local_target.go:prepareFindNodeOwnedLocalTargetConstruction"] = 3
	expected["findnode"]["find_node_owned_local_target.go:dispatchOwnedFindNodeLocalTarget"] = 10
	expected["findnode"]["find_node_owned_local_target.go:runOwnedFindNodeLocalWork"] = 1
	expected["findnode"]["find_node_owned_local_target.go:completeOwnedFindNodeLocalSignature"] = 3
	// Exact private Ready constructor sites; all existing closures stay strict.
	expected["proofTransportServiceToken"]["find_node_ready_construction.go:<package>"] = 1
	expected["findNode"]["find_node_ready_construction.go:prepareFindNodeReadyConstruction"] = 2
	expected["findnode"]["find_node_ready_construction.go:prepareFindNodeReadyConstruction"] = 4
	expected["findnode"]["find_node_ready_construction.go:newFindNodeReadyCandidateService"] = 1
	expected["findnode"]["find_node_ready_construction.go:buildWithOwnDM"] = 2
	expected["proofToken"]["find_node_ready_construction.go:bindings"] = 1
	expected["findNode"]["find_node_ready_construction.go:bindings"] = 8
	expected["findnode"]["find_node_ready_construction.go:bindings"] = 3
	expected["findnode"]["find_node_ready_construction.go:checkBeforeInstall"] = 2
	expected["findnode"]["find_node_ready_construction.go:finalize"] = 3
	expected["findnode"]["identity_pin_own_dm_runtime.go:<package>"] = 2
	actual := map[string]map[string]int{}
	made := map[string]map[string]int{}
	ends := map[string]int{}
	methods := map[string]map[string]int{"sendCapturedInbound": {}, "closeCapturedInbound": {}, "attempt": {}}
	for name := range expected {
		actual[name] = map[string]int{}
	}
	for name := range constructors {
		made[name] = map[string]int{}
	}
	var failures []string
	directOnly := map[string]bool{"runWithFiles": true, "selectInboundSend": true, "selectInboundClose": true, "bindInboundLeafLocked": true, "inboundTerminalForWire": true, "transportTerminalLocked": true, "selectInboundPinAfterLocked": true, "endInboundLeavesLocked": true, "staleInboundTerminalsLocked": true, "condemnInboundSelected": true, "stageInboundPin": true, "publishInboundLifetime": true, "acceptedTransportArrivalContext": true, "transportArrivalOwner": true, "newServiceWithDatagramConstructor": true, "newInboundPinCandidateService": true, "newUnknownInboundPinCandidateService": true, "attachInboundPinCandidate": true, "installUnknownInboundPinJournal": true, "newFindNodeCandidateService": true, "newFindNodeInboundCandidateService": true, "captureFindNodeSeed": true, "publishFindNodeLocked": true, "publishAcceptedProofTransport": true, "prepareFindNodeCMAcceptedLocked": true, "publishFindNodeCM": true, "findNodeMembers": true, "bindProofTransportLocked": true, "bindProofTransport": true, "endProofTransportIO": true, "endProofTransport": true, "ownerLocked": true, "captureSessionTransportOwner": true, "sessionTransportArrivalContext": true, "matchesTransportLocked": true, "PrepareSession": true, "Bind": true, "End": true, "NewSession": true}
	for path, raw := range sources {
		f, err := parser.ParseFile(token.NewFileSet(), path, raw, 0)
		if err != nil {
			return []string{err.Error()}
		}
		for _, decl := range f.Decls {
			site := path + ":<package>"
			var declaration *ast.Ident
			if fn, ok := decl.(*ast.FuncDecl); ok {
				site = path + ":" + fn.Name.Name
				declaration = fn.Name
			}
			callees := map[ast.Node]bool{}
			selectorParents := map[*ast.Ident][]*ast.SelectorExpr{}
			ast.Inspect(decl, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					callees[c.Fun] = true
				}
				if parent, ok := n.(*ast.SelectorExpr); ok {
					// Keep the original traversal order and parent multiplicity.
					selectorParents[parent.Sel] = append(selectorParents[parent.Sel], parent)
				}
				return true
			})
			ast.Inspect(decl, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.Ident:
					if n == declaration {
						return true
					}
					if refs, ok := actual[n.Name]; ok {
						refs[site]++
					}
					if directOnly[n.Name] {
						// A selector's method identifier belongs to its parent selector;
						// bare identifiers must themselves be a direct call target.
						parents := selectorParents[n]
						for _, parent := range parents {
							if !callees[parent] {
								failures = append(failures, "method value escape "+site+":"+n.Name)
							}
						}
						if len(parents) == 0 && !callees[n] {
							failures = append(failures, "function value escape "+site+":"+n.Name)
						}
					}
				case *ast.SelectorExpr:
					if refs, tracked := methods[n.Sel.Name]; tracked {
						refs[site]++
						if !callees[n] {
							failures = append(failures, "selection method value escape "+site)
						}
					}
					if n.Sel.Name == "end" {
						ends[site]++
						if !callees[n] {
							failures = append(failures, "terminal method value escape "+site)
						}
					}
				case *ast.CompositeLit:
					if id, ok := n.Type.(*ast.Ident); ok {
						if refs, tracked := made[id.Name]; tracked {
							refs[site]++
						}
					}
				case *ast.CallExpr:
					if id, ok := n.Fun.(*ast.Ident); ok && id.Name == "new" && len(n.Args) == 1 {
						if typ, ok := n.Args[0].(*ast.Ident); ok {
							if _, tracked := constructors[typ.Name]; tracked {
								failures = append(failures, "unreviewed new authority "+site)
							}
						}
					}
				}
				return true
			})
		}
	}
	for name, want := range expected {
		if !reflect.DeepEqual(actual[name], want) {
			failures = append(failures, fmt.Sprintf("N1 exact reference %s got%v want%v", name, actual[name], want))
		}
	}
	for name, want := range constructors {
		if !reflect.DeepEqual(made[name], want) {
			failures = append(failures, fmt.Sprintf("N1 constructor %s got%v want%v", name, made[name], want))
		}
	}
	if !reflect.DeepEqual(methods, map[string]map[string]int{"sendCapturedInbound": {"network_bridge.go:sendFrameBound": 1}, "closeCapturedInbound": {"network_bridge.go:Close": 1}, "attempt": {"identity_pin_inbound_lifecycle.go:runWithFiles": 1, "identity_pin_inbound_lifecycle.go:pass": 1}}) {
		failures = append(failures, "captured selection completion roots changed")
	}
	if !reflect.DeepEqual(ends, expectedEnds) {
		failures = append(failures, fmt.Sprintf("terminal inventory got%v want%v", ends, expectedEnds))
	}
	return failures
}

func ninN1Sources(t *testing.T) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string][]byte{}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".go") && !strings.HasSuffix(e.Name(), "_test.go") {
			raw, err := os.ReadFile(e.Name())
			if err != nil {
				t.Fatal(err)
			}
			sources[e.Name()] = raw
		}
	}
	return sources
}

func TestInboundN1ClosedOriginalOwnerAuthorityInventory(t *testing.T) {
	sources := ninN1Sources(t)
	if failures := ninN1SourceErrors(sources); len(failures) != 0 {
		t.Fatal(failures)
	}
	// These are LANGUAGE-VALID function/type/value escape counterexamples.
	// The shim only typechecks the negative syntax; it supplies no proof or
	// fixture authority to production. Actual candidate tests use real TLS.
	for _, extra := range []string{
		`var alias=newInboundPinCandidateService`,
		`var unknownAlias=newUnknownInboundPinCandidateService`,
		`var attachAlias=attachInboundPinCandidate`,
		`var installAlias=(*secureSessionStore).installUnknownInboundPinJournal`,
		`type ResultAlias = ninInstallResult`,
		`func extra(s *secureSessionStore){_=s.stageInboundPin}`,
		`func extra(){_=new(matureInboundWitness)}`,
		`type Escape = proofTransportOwner`,
		`type ProducerAlias = inboundPinProducer`,
		`var keyAlias=proofTransportArrivalKey{}`,
		`var clocks=[]inboundPinClock{{}}`,
		`func extra(s *Service){_=s.publishInboundLifetime}`,
		`func extra(p *inboundPinProducer){_=p.runWithFiles}`,
		`func extra(p *inboundPinProducer){_=p.attempt}`,
		`func extra(selected inboundSendSelection){_=selected.sendCapturedInbound}`,
		`func extra(selected inboundCloseSelection){_=selected.closeCapturedInbound}`,
	} {
		shim := `package fixture;type secureSessionStore struct{};func(*secureSessionStore)stageInboundPin(){};func(*secureSessionStore)installUnknownInboundPinJournal(){};type ninInstallResult struct{};type Service struct{};func(*Service)publishInboundLifetime(){};type matureInboundWitness struct{};type proofTransportOwner struct{};type proofTransportArrivalKey struct{};type inboundPinClock struct{};type inboundPinProducer struct{};func(*inboundPinProducer)attempt(){};func(*inboundPinProducer)runWithFiles(){};type inboundSendSelection struct{};func(inboundSendSelection)sendCapturedInbound(){};type inboundCloseSelection struct{};func(inboundCloseSelection)closeCapturedInbound(){};func newInboundPinCandidateService(){};func newUnknownInboundPinCandidateService(){};func attachInboundPinCandidate(){};` + extra
		fs := token.NewFileSet()
		parsed, err := parser.ParseFile(fs, "valid-escape.go", shim, 0)
		if err != nil {
			t.Fatal(err)
		}
		checker := types.Config{}
		if _, err := checker.Check("fixture", fs, []*ast.File{parsed}, nil); err != nil {
			t.Fatal("escape must compile before guard rejects it", err)
		}
		mutated := map[string][]byte{}
		for k, v := range sources {
			mutated[k] = v
		}
		mutated["extra.go"] = []byte("package node\n" + extra)
		if len(ninN1SourceErrors(mutated)) == 0 {
			t.Fatal("accepted extra function/type/factory/alias root", extra)
		}
	}
	// Same approved file, wrong AST function is still forbidden.
	mutated := map[string][]byte{}
	for k, v := range sources {
		mutated[k] = v
	}
	mutated["identity_pin_inbound_lifecycle.go"] = append(append([]byte(nil), sources["identity_pin_inbound_lifecycle.go"]...), []byte("\nfunc extraSameFile(s *secureSessionStore){_ = s.stageInboundPin}\n")...)
	if len(ninN1SourceErrors(mutated)) == 0 {
		t.Fatal("whole approved source file became an allowance")
	}
}

// Exact synchronous incoming edges. No context/owner is added to any outgoing
// DTO. This checks the named integration contract, not universal data flow.
func TestInboundN1ArrivalAndBackgroundOwnerEdgesStayClosed(t *testing.T) {
	sources := ninN1Sources(t)
	function := func(path, name string) *ast.FuncDecl {
		parsed, err := parser.ParseFile(token.NewFileSet(), path, sources[path], 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			if f, ok := decl.(*ast.FuncDecl); ok && f.Name.Name == name {
				if name == "Handle" {
					if f.Recv == nil || len(f.Recv.List) != 1 {
						continue
					}
					pointer, ok := f.Recv.List[0].Type.(*ast.StarExpr)
					if !ok {
						continue
					}
					receiver, ok := pointer.X.(*ast.Ident)
					if !ok || receiver.Name != "pushIdentityHandler" {
						continue
					}
				}
				return f
			}
		}
		t.Fatal("missing reviewed lifecycle function", path, name)
		return nil
	}
	requireCall := func(path, name, target string, args []string) {
		f := function(path, name)
		count := 0
		ast.Inspect(f.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != target {
				return true
			}
			count++
			if len(call.Args) != len(args) {
				t.Fatal("changed original-owner argument arity", name, target)
			}
			for k, want := range args {
				if want == "" {
					continue
				}
				arg, ok := call.Args[k].(*ast.Ident)
				if !ok || arg.Name != want {
					t.Fatal("lost original synchronous owner/context", name, target, k)
				}
			}
			return true
		})
		if count != 1 {
			t.Fatal("changed exact owned edge", name, target, count)
		}
	}
	requireCall("datagram_integration.go", "dispatchInboundDatagramLineOwned", "handleDatagramFrame", []string{"ctx", "wire", ""})
	requireCall("datagram_integration.go", "handleDatagramFrame", "HandleInbound", []string{"ctx", ""})
	requireCall("datagram_integration.go", "dispatchInboundDatagramWireOwned", "dispatchInboundDatagramLineOwned", []string{"connID", "wire", "owner"})
	requireCall("service.go", "handleCommandOwned", "dispatchNetworkFrameOwned", []string{"connID", "wire", "owner"})
	requireCall("service.go", "handleConn", "handleCommandOwned", []string{"connID", "line", "owner"})
	requireCall("service.go", "startInboundSessionDeliveryOwned", "sendInitialIdentityPushOwned", []string{"", "pushPeer", "", "owner"})
	requireCall("service.go", "handleConn", "inboundHeartbeatOwned", []string{"connID", "addr", "heartbeatStop", "owner"})
	// A new async incoming delivery would retain the capsule beyond this stack.
	for _, pair := range [][2]string{{"datagram_integration.go", "dispatchInboundDatagramLineOwned"}, {"datagram_integration.go", "handleDatagramFrame"}, {"identity_discovery.go", "Handle"}} {
		f := function(pair[0], pair[1])
		ast.Inspect(f.Body, func(n ast.Node) bool {
			if _, ok := n.(*ast.GoStmt); ok {
				t.Fatal("incoming capsule gained detached delivery", pair)
			}
			return true
		})
	}
	// Value shapes of existing outgoing owners remain independent of N1. The
	// package-wide root inventory separately forbids a leaf/handle type escape.
	for _, path := range []string{"../datagram/queue.go", "../datagram/queue_emitter.go", "../netcore/write_ticket.go"} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "inboundArrival") || strings.Contains(string(raw), "proofTransportArrival") || strings.Contains(string(raw), "proofTransportOwner") || strings.Contains(string(raw), "matureInboundWitness") {
			t.Fatal("outgoing owner retained incoming lifetime", path)
		}
	}
}

func TestInboundN1HandlerEndDeferPrecedesActualHeartbeatJoin(t *testing.T) {
	raw, err := os.ReadFile("service.go")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), "service.go", raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	var handler *ast.FuncDecl
	for _, decl := range parsed.Decls {
		if f, ok := decl.(*ast.FuncDecl); ok && f.Name.Name == "handleConn" {
			handler = f
		}
	}
	if handler == nil {
		t.Fatal("actual handler missing")
	}
	endIndex, joinIndex := -1, -1
	for index, stmt := range handler.Body.List {
		deferred, ok := stmt.(*ast.DeferStmt)
		if !ok {
			continue
		}
		if sel, ok := deferred.Call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "end" {
			if receiver, ok := sel.X.(*ast.Ident); ok && receiver.Name == "owner" {
				endIndex = index
			}
		}
		if body, ok := deferred.Call.Fun.(*ast.FuncLit); ok {
			ast.Inspect(body.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Wait" {
						if receiver, ok := sel.X.(*ast.Ident); ok && receiver.Name == "heartbeatDone" {
							joinIndex = index
						}
					}
				}
				return true
			})
		}
	}
	if joinIndex < 0 || endIndex <= joinIndex {
		t.Fatal("LIFO End does not run before actual heartbeat join", endIndex, joinIndex)
	}
}

// Exact named shared factors, not a general alias/dataflow verifier. Holding
// Service/ConnID/callback state in the selections or redoing a registry lookup
// after capture is outside this reviewed contract even in this approved file.
func ninN1SelectionShapeErrors(path string, raw []byte) []string {
	if path != "network_bridge.go" && path != "identity_pin_inbound_lifecycle.go" {
		return []string{"unreviewed selection-shape source"}
	}
	expected := map[string]string{
		"inboundSendSelection":  `type inboundSendSelection struct { core *netcore.NetCore; owner *proofTransportOwner }`,
		"inboundCloseSelection": `type inboundCloseSelection struct { core *netcore.NetCore }`,
		"selectInboundSend": `func (b *networkBridge) selectInboundSend(id domain.ConnID) inboundSendSelection {
 b.svc.peerMu.RLock()
 selected := inboundSendSelection{core:b.svc.coreForIDLocked(id), owner:b.svc.transportTerminalLocked(id)}
 b.svc.peerMu.RUnlock()
 return selected
 }`,
		"sendCapturedInbound": `func (selected inboundSendSelection) sendCapturedInbound(ctx context.Context, frame []byte, synchronous bool) (*proofTransportOwner,error) {
 if selected.core==nil {return nil,netcore.ErrUnknownConn}
 if synchronous {return selected.owner,ctxSendStatusToError(ctx,selected.core.SendRawSyncCtx(ctx,frame))}
 return selected.owner,netcore.SendStatusToError(selected.core.SendRaw(frame))
 }`,
		"sendFrameBound": `func (b *networkBridge) sendFrameBound(ctx context.Context,id domain.ConnID,frame []byte,synchronous bool)(*proofTransportOwner,error){
 if err:=ctx.Err();err!=nil{return nil,err}
 selected:=b.selectInboundSend(id)
 return selected.sendCapturedInbound(ctx,frame,synchronous)
 }`,
		"selectInboundClose": `func (b *networkBridge) selectInboundClose(id domain.ConnID) inboundCloseSelection {
 b.svc.peerMu.RLock()
 selected:=inboundCloseSelection{core:b.svc.coreForIDLocked(id)}
 owner:=b.svc.transportTerminalLocked(id)
 owner.end()
 b.svc.peerMu.RUnlock()
 return selected
 }`,
		"closeCapturedInbound": `func(selected inboundCloseSelection)closeCapturedInbound()error{
 if selected.core==nil{return netcore.ErrUnknownConn}
 selected.core.Close()
 return nil
 }`,
		"Close": `func(b *networkBridge)Close(_ context.Context,id domain.ConnID)error{
 selected:=b.selectInboundClose(id)
 return selected.closeCapturedInbound()
 }`,
		"inboundPinProducer": `type inboundPinProducer struct{service *Service;store *secureSessionStore;wake chan struct{};passMu sync.Mutex;cursor domain.ConnID}`,
		"run":                `func(p *inboundPinProducer)run(ctx context.Context){p.runWithFiles(ctx,nil)}`,
		"pass":               `func(p *inboundPinProducer)pass(ctx context.Context)error{return p.attempt(ctx,nil)}`,
	}
	// The two actual runtime wrappers above MUST supply nil; the borrowed typed
	// port belongs only to synchronous private fixture invocations, never a field.
	actual := map[string][]string{}
	parsed, err := parser.ParseFile(token.NewFileSet(), "reviewed-shape.go", raw, 0)
	if err != nil {
		return []string{err.Error()}
	}
	canonical := func(decl ast.Decl) string {
		var b bytes.Buffer
		if err := ast.Fprint(&b, nil, decl, func(name string, value reflect.Value) bool {
			return value.Type() != reflect.TypeFor[token.Pos]() && name != "Obj" && name != "Doc" && name != "Comment"
		}); err != nil {
			return "FORMAT_ERROR:" + err.Error()
		}
		return b.String()
	}
	for _, decl := range parsed.Decls {
		switch decl := decl.(type) {
		case *ast.FuncDecl:
			if _, tracked := expected[decl.Name.Name]; tracked {
				actual[decl.Name.Name] = append(actual[decl.Name.Name], canonical(decl))
			}
		case *ast.GenDecl:
			for _, spec := range decl.Specs {
				if typ, ok := spec.(*ast.TypeSpec); ok {
					if _, tracked := expected[typ.Name.Name]; tracked {
						value := canonical(&ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{typ}})
						if typ.Assign.IsValid() {
							value += " ALIAS_FORBIDDEN"
						}
						actual[typ.Name.Name] = append(actual[typ.Name.Name], value)
					}
				}
			}
		}
	}
	var failures []string
	for name, text := range expected {
		// Each invocation is one of the two exact files; require only roots owned
		// by that file. No new file-wide allowance results from this split.
		worker := name == "run" || name == "pass" || name == "inboundPinProducer"
		if worker != (path == "identity_pin_inbound_lifecycle.go") {
			continue
		}
		want, err := parser.ParseFile(token.NewFileSet(), "literal-expected.go", "package node\n"+text, 0)
		if err != nil {
			return []string{"invalid reviewed literal:" + err.Error()}
		}
		if len(actual[name]) != 1 || actual[name][0] != canonical(want.Decls[0]) {
			failures = append(failures, "captured selection/runtime wrapper shape changed:"+name)
		}
	}
	return failures
}

func TestInboundN1SelectionWrappersAndNilRuntimePortsStayExact(t *testing.T) {
	sources := ninN1Sources(t)
	for _, path := range []string{"network_bridge.go", "identity_pin_inbound_lifecycle.go"} {
		if failures := ninN1SelectionShapeErrors(path, sources[path]); len(failures) != 0 {
			t.Fatal(path, failures)
		}
	}
	// Typecheck coherent negative snippets independently. This is a language/
	// contract control; it is not a TLS proof or actual registry ABA witness.
	for _, body := range []string{
		`func(b *networkBridge)sendFrameBound(ctx Context,id ID,frame []byte,sync bool)(*Owner,error){selected:=b.selectInboundSend(id);_,err:=selected.sendCapturedInbound(ctx,frame,sync);return b.selectInboundSend(id).owner,err}`,
		`func(b *networkBridge)Close(ctx Context,id ID)error{selected:=b.selectInboundClose(id);_=selected;b.coreForID(id).Close(); return nil}`,
		`func(b *networkBridge)extra(){_=b.selectInboundSend}`,
	} {
		shim := `package fixture;type Context struct{};type ID int;type Owner struct{};type Core struct{};func(*Core)Close(){};type inboundSendSelection struct{owner *Owner};func(inboundSendSelection)sendCapturedInbound(Context,[]byte,bool)(*Owner,error){return nil,nil};type inboundCloseSelection struct{};type networkBridge struct{};func(*networkBridge)selectInboundSend(ID)inboundSendSelection{return inboundSendSelection{}};func(*networkBridge)selectInboundClose(ID)inboundCloseSelection{return inboundCloseSelection{}};func(*networkBridge)coreForID(ID)*Core{return nil};` + body
		fs := token.NewFileSet()
		parsed, err := parser.ParseFile(fs, "valid-late-lookup.go", shim, 0)
		if err != nil {
			t.Fatal(err)
		}
		checker := types.Config{}
		if _, err := checker.Check("fixture", fs, []*ast.File{parsed}, nil); err != nil {
			t.Fatal("negative selection form must compile", err)
		}
	}
	changes := [][2]string{
		{"return selected.sendCapturedInbound(ctx, frame, synchronous)", "_, err := selected.sendCapturedInbound(ctx, frame, synchronous); return b.selectInboundSend(id).owner, err"},
		{"return selected.closeCapturedInbound()", "_ = selected; b.coreForID(id).Close(); return nil"},
		{"core  *netcore.NetCore\n\towner *proofTransportOwner", "core *netcore.NetCore; owner *proofTransportOwner; svc *Service; id domain.ConnID"},
	}
	for _, change := range changes {
		raw := string(sources["network_bridge.go"])
		if strings.Count(raw, change[0]) != 1 {
			t.Fatal("negative literal not unique", change[0])
		}
		modified := strings.Replace(raw, change[0], change[1], 1)
		if failures := ninN1SelectionShapeErrors("network_bridge.go", []byte(modified)); len(failures) == 0 {
			t.Fatal("accepted late lookup or retained registry owner in selection", change[0])
		}
	}
	// A method-value in the approved file but outside the exact node is also an
	// escape. The completion-root counts and direct-callee inventory both reject.
	modified := map[string][]byte{}
	for k, v := range sources {
		modified[k] = v
	}
	modified["network_bridge.go"] = append(append([]byte(nil), sources["network_bridge.go"]...), []byte("\nfunc extraSelectionEscape(b *networkBridge){_=b.selectInboundSend}\n")...)
	if len(ninN1SourceErrors(modified)) == 0 {
		t.Fatal("approved file allowed an extra selection method-value root")
	}
	for _, change := range [][2]string{
		{"p.runWithFiles(ctx, nil)", "p.runWithFiles(ctx, retainedPort)"},
		{"return p.attempt(ctx, nil)", "return p.attempt(ctx, retainedPort)"},
	} {
		raw := string(sources["identity_pin_inbound_lifecycle.go"])
		if strings.Count(raw, change[0]) != 1 {
			t.Fatal("nil-wrapper literal not unique")
		}
		if len(ninN1SelectionShapeErrors("identity_pin_inbound_lifecycle.go", []byte(strings.Replace(raw, change[0], change[1], 1)))) == 0 {
			t.Fatal("normal runtime gained a borrowed/retained fixture dependency")
		}
	}
}

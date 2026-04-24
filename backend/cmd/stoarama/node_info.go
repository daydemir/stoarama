package main

type nodeMeResponse struct {
	Node struct {
		ID          int64  `json:"id"`
		NodeType    string `json:"node_type"`
		DisplayName string `json:"display_name"`
		Hostname    string `json:"hostname"`
		Platform    string `json:"platform"`
	} `json:"node"`
}

func mustLoadNodeInfo(baseURL, token string) nodeMeResponse {
	var resp nodeMeResponse
	if err := apiRequest("GET", normalizeBaseURL(baseURL)+"/api/v1/node/me", nil, token, &resp); err != nil {
		fatalf("load node info: %v", err)
	}
	return resp
}

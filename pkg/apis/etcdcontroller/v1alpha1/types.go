/*
Copyright 2024 The etcd Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EtcdCluster is a specification for etcd cluster resource
type EtcdCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdClusterSpec   `json:"spec"`
	Status EtcdClusterStatus `json:"status"`
}

// EtcdClusterSpec is the spec for EtcdCluster resource
type EtcdClusterSpec struct {
	// Size is the expected size of the etcd cluster.
	Size int `json:"size"`
	// Registry is the name of the registry that hosts etcd container images.
	// Defaults to "gcr.io/etcd-development/etcd".
	Registry string `json:"registry,omitempty"`
	// Version is the expected version of the etcd container image.
	Version string `json:"version"`
}

// EtcdClusterStatus is the status for EtcdCluster resource
type EtcdClusterStatus struct {
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EtcdClusterList is a list of EtcdCluster resources
type EtcdClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []EtcdCluster `json:"items"`
}

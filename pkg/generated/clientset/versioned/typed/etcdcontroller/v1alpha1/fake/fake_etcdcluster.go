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

// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	"context"

	v1alpha1 "github.com/ahrtr/etcd-operator/pkg/apis/etcdcontroller/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeEtcdClusters implements EtcdClusterInterface
type FakeEtcdClusters struct {
	Fake *FakeClustersV1alpha1
	ns   string
}

var etcdclustersResource = v1alpha1.SchemeGroupVersion.WithResource("etcdclusters")

var etcdclustersKind = v1alpha1.SchemeGroupVersion.WithKind("EtcdCluster")

// Get takes name of the etcdCluster, and returns the corresponding etcdCluster object, and an error if there is any.
func (c *FakeEtcdClusters) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha1.EtcdCluster, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(etcdclustersResource, c.ns, name), &v1alpha1.EtcdCluster{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.EtcdCluster), err
}

// List takes label and field selectors, and returns the list of EtcdClusters that match those selectors.
func (c *FakeEtcdClusters) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha1.EtcdClusterList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(etcdclustersResource, etcdclustersKind, c.ns, opts), &v1alpha1.EtcdClusterList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1alpha1.EtcdClusterList{ListMeta: obj.(*v1alpha1.EtcdClusterList).ListMeta}
	for _, item := range obj.(*v1alpha1.EtcdClusterList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested etcdClusters.
func (c *FakeEtcdClusters) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(etcdclustersResource, c.ns, opts))

}

// Create takes the representation of a etcdCluster and creates it.  Returns the server's representation of the etcdCluster, and an error, if there is any.
func (c *FakeEtcdClusters) Create(ctx context.Context, etcdCluster *v1alpha1.EtcdCluster, opts v1.CreateOptions) (result *v1alpha1.EtcdCluster, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(etcdclustersResource, c.ns, etcdCluster), &v1alpha1.EtcdCluster{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.EtcdCluster), err
}

// Update takes the representation of a etcdCluster and updates it. Returns the server's representation of the etcdCluster, and an error, if there is any.
func (c *FakeEtcdClusters) Update(ctx context.Context, etcdCluster *v1alpha1.EtcdCluster, opts v1.UpdateOptions) (result *v1alpha1.EtcdCluster, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(etcdclustersResource, c.ns, etcdCluster), &v1alpha1.EtcdCluster{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.EtcdCluster), err
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *FakeEtcdClusters) UpdateStatus(ctx context.Context, etcdCluster *v1alpha1.EtcdCluster, opts v1.UpdateOptions) (*v1alpha1.EtcdCluster, error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateSubresourceAction(etcdclustersResource, "status", c.ns, etcdCluster), &v1alpha1.EtcdCluster{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.EtcdCluster), err
}

// Delete takes name of the etcdCluster and deletes it. Returns an error if one occurs.
func (c *FakeEtcdClusters) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteActionWithOptions(etcdclustersResource, c.ns, name, opts), &v1alpha1.EtcdCluster{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeEtcdClusters) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(etcdclustersResource, c.ns, listOpts)

	_, err := c.Fake.Invokes(action, &v1alpha1.EtcdClusterList{})
	return err
}

// Patch applies the patch and returns the patched etcdCluster.
func (c *FakeEtcdClusters) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.EtcdCluster, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(etcdclustersResource, c.ns, name, pt, data, subresources...), &v1alpha1.EtcdCluster{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.EtcdCluster), err
}

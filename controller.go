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

package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"golang.org/x/time/rate"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	clientv3 "go.etcd.io/etcd/client/v3"

	ecv1alpha1 "github.com/ahrtr/etcd-operator/pkg/apis/etcdcontroller/v1alpha1"
	clientset "github.com/ahrtr/etcd-operator/pkg/generated/clientset/versioned"
	customScheme "github.com/ahrtr/etcd-operator/pkg/generated/clientset/versioned/scheme"
	informers "github.com/ahrtr/etcd-operator/pkg/generated/informers/externalversions/etcdcontroller/v1alpha1"
	listers "github.com/ahrtr/etcd-operator/pkg/generated/listers/etcdcontroller/v1alpha1"
)

const controllerAgentName = "etcd-operator"

const (
	// SuccessSynced is used as part of the Event 'reason' when an EtcdCluster is synced
	SuccessSynced = "Synced"
	// ErrResourceExists is used as part of the Event 'reason' when an EtcdCluster fails
	// to sync due to a Statefulsets of the same name already existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a Statefulsets already existing
	MessageResourceExists = "Resource %q already exists and is not managed by EtcdCluster"
	// MessageResourceSynced is the message used for an Event fired when an EtcdCluster
	// is synced successfully
	MessageResourceSynced = "EtcdCluster synced successfully"
)

// Controller is the controller implementation for EtcdCluster resources
type Controller struct {
	// kubeClientset is a standard kubernetes clientset
	kubeClientset kubernetes.Interface
	// customClientset is a clientset for our own API group
	customClientset clientset.Interface

	statefulsetsLister appslisters.StatefulSetLister
	statefulsetsSynced cache.InformerSynced
	etcdClusterLister  listers.EtcdClusterLister
	etcdClusterSynced  cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder
}

// NewController returns a new EtcdCluster controller
func NewController(
	ctx context.Context,
	kubeclientset kubernetes.Interface,
	customClientset clientset.Interface,
	statefulsetsInformer appsinformers.StatefulSetInformer,
	etcdClusterInformer informers.EtcdClusterInformer) *Controller {
	logger := klog.FromContext(ctx)

	// Create event broadcaster
	// Add EtcdCluster types to the default Kubernetes Scheme so Events can be
	// logged for EtcdCluster types.
	utilruntime.Must(customScheme.AddToScheme(scheme.Scheme))
	logger.V(4).Info("Creating event broadcaster")

	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})
	ratelimiter := workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(50), 300)},
	)

	controller := &Controller{
		kubeClientset:      kubeclientset,
		customClientset:    customClientset,
		statefulsetsLister: statefulsetsInformer.Lister(),
		statefulsetsSynced: statefulsetsInformer.Informer().HasSynced,
		etcdClusterLister:  etcdClusterInformer.Lister(),
		etcdClusterSynced:  etcdClusterInformer.Informer().HasSynced,
		workqueue:          workqueue.NewRateLimitingQueue(ratelimiter),
		recorder:           recorder,
	}

	logger.Info("Setting up event handlers")
	// Set up an event handler for when EtcdCluster resources change
	etcdClusterInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueEtcdCluster,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueEtcdCluster(new)
		},
	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shut down the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(ctx context.Context, workers int) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()
	logger := klog.FromContext(ctx)

	// Start the informer factories to begin populating the informer caches
	logger.Info("Starting EtcdCluster controller")

	// Wait for the caches to be synced before starting workers
	logger.Info("Waiting for informer caches to sync")

	if ok := cache.WaitForCacheSync(ctx.Done(), c.statefulsetsSynced, c.etcdClusterSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	logger.Info("Starting workers", "count", workers)
	// Launch two workers to process EtcdCluster resources
	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.runWorker, time.Second)
	}

	logger.Info("Started workers")
	<-ctx.Done()
	logger.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	obj, shutdown := c.workqueue.Get()
	logger := klog.FromContext(ctx)

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// EtcdCluster resource to be synced.
		if err := c.syncHandler(ctx, key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(obj)
		logger.Info("Successfully synced", "resourceName", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
	}

	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two.
func (c *Controller) syncHandler(ctx context.Context, key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "resourceName", key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the EtcdCluster resource with this namespace/name
	ec, err := c.etcdClusterLister.EtcdClusters(namespace).Get(name)
	if err != nil {
		// The EtcdCluster resource may no longer exist, in which case we stop processing.
		if errors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("EtcdCluster '%s' in work queue no longer exists", key))
			return nil
		}
		return err
	}

	logger.Info("Syncing etcdcluster", "Spec", ec.Spec)

	// Get the statefulsets which has the same name as the EtcdCluster resource
	sts, err := c.statefulsetsLister.StatefulSets(ec.Namespace).Get(ec.Name)
	// If the resource doesn't exist, we'll create it with replica = 0.
	if errors.IsNotFound(err) {
		// The ec.Spec.Size should be always > 0, because the validation
		// defined in crd requires it must have a minimum value of 1 and
		// a maximum value of 10. But the reconcile loop should be correct
		// by itself, so let's keep the check.
		if ec.Spec.Size > 0 {
			logger.Info("Creating statefulsets with 0 replica", "expectedSize", ec.Spec.Size)
			sts, err = c.kubeClientset.AppsV1().StatefulSets(ec.Namespace).Create(context.TODO(), newStatefulsets(ec, 0), metav1.CreateOptions{})
		} else {
			logger.Info("Skipping creating statefulsets due to the expected cluster size being 0")
			return nil
		}
	}

	// If an error occurs during Get/Create, we'll requeue the item so we can
	// attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		return err
	}

	// If the Statefulsets is not controlled by this EtcdCluster resource, we should log
	// a warning to the event recorder and return error msg.
	if !metav1.IsControlledBy(sts, ec) {
		msg := fmt.Sprintf(MessageResourceExists, sts.Name)
		c.recorder.Event(ec, corev1.EventTypeWarning, ErrResourceExists, msg)
		return fmt.Errorf("%s", msg)
	}

	memberListResp, healthInfos, err := healthCheck(ec, sts, logger)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	memberCnt := 0
	if memberListResp != nil {
		memberCnt = len(memberListResp.Members)
	}
	replica := int(*sts.Spec.Replicas)

	if replica != memberCnt {
		// TODO: finish the logic later
		if replica < memberCnt {
			// a new added learner hasn't started yet

			// re-generate configuration for the new learner member;
			// increase statefulsets's replica by 1
		} else {
			// an already removed member hasn't stopped yet.

			// Decrease the statefulsets's replica by 1
		}
		// return
	}

	if memberCnt != len(healthInfos) {
		return fmt.Errorf("memberCnt (%d) isn't equal to healthy member count (%d)", memberCnt, len(healthInfos))
	}
	// There should be at most one learner, namely the last one
	if memberCnt > 0 && healthInfos[memberCnt-1].Status.IsLearner {
		logger = logger.WithValues("replica", replica, "expectedSize", ec.Spec.Size)

		learnerStatus := healthInfos[memberCnt-1].Status

		var leaderStatus *clientv3.StatusResponse
		for i := 0; i < memberCnt-1; i++ {
			status := healthInfos[i].Status
			if status.Leader == status.Header.MemberId {
				leaderStatus = status
				break
			}
		}

		if leaderStatus == nil {
			return fmt.Errorf("couldn't find leader, memberCnt: %d", memberCnt)
		}

		learnerID := healthInfos[memberCnt-1].Status.Header.MemberId
		if isLearnerReady(leaderStatus, learnerStatus) {
			logger.Info("Promoting the learner member", "learnerID", learnerID)
			eps := clientEndpointsFromStatefulsets(sts)
			eps = eps[:(len(eps) - 1)]
			return promoteLearner(eps, learnerID)
		}

		logger.Info("The learner member isn't ready to be promoted yet", "learnerID", learnerID)

		return nil
	}

	expectedSize := ec.Spec.Size
	if replica == expectedSize {
		// TODO: check version change, and perform upgrade if needed.
		return nil
	}

	var targetReplica int32
	if replica < expectedSize {
		// scale out
		targetReplica = int32(replica + 1)
		logger = logger.WithValues("targetReplica", targetReplica, "expectedSize", ec.Spec.Size)

		// TODO: check PV & PVC for the new member. If they already exist,
		// then it means they haven't been cleaned up yet when scaling in.

		_, peerURL := peerEndpointForOrdinalIndex(ec, replica)
		if replica > 0 {
			// if replica == 0, then it's the very first member, then
			// there is no need to add it as a learner; instead we can
			// start it as a voting member directly.
			eps := clientEndpointsFromStatefulsets(sts)
			logger.Info("[Scale out] adding a new learner member", "peerURLs", peerURL)
			if _, err := addMember(eps, []string{peerURL}, true); err != nil {
				return err
			}
		} else {
			logger.Info("[Scale out] Starting the very first voting member", "peerURLs", peerURL)
		}
	} else {
		// scale in
		targetReplica = int32(replica - 1)
		logger = logger.WithValues("targetReplica", targetReplica, "expectedSize", ec.Spec.Size)

		memberID := healthInfos[memberCnt-1].Status.Header.MemberId

		logger.Info("[Scale in] removing one member", "memberID", memberID)
		eps := clientEndpointsFromStatefulsets(sts)
		eps = eps[:targetReplica]
		if err := removeMember(eps, memberID); err != nil {
			return err
		}
	}

	logger.Info("Applying etcd cluster state")
	if err := c.applyEtcdClusterState(ec, int(targetReplica)); err != nil {
		return err
	}

	logger.Info("Updating statefulsets")
	sts, err = c.kubeClientset.AppsV1().StatefulSets(ec.Namespace).Update(context.TODO(), newStatefulsets(ec, targetReplica), metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	c.recorder.Event(ec, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
	return nil
}

func isLearnerReady(leaderStatus, learnerStatus *clientv3.StatusResponse) bool {
	leaderRev := leaderStatus.Header.Revision
	learnerRev := learnerStatus.Header.Revision

	learnerReadyPercent := float64(learnerRev) / float64(leaderRev)
	return learnerReadyPercent >= 0.9
}

// healthCheck returns a memberList and an error.
// If any member (excluding not yet started or already removed member)
// is unhealthy, the error won't be nil.
func healthCheck(ec *ecv1alpha1.EtcdCluster, sts *appsv1.StatefulSet, lg klog.Logger) (*clientv3.MemberListResponse, []epHealth, error) {
	replica := int(*sts.Spec.Replicas)
	if replica == 0 {
		return nil, nil, nil
	}

	endpoints := clientEndpointsFromStatefulsets(sts)

	memberlistResp, err := memberList(endpoints)
	if err != nil {
		return nil, nil, err
	}
	memberCnt := len(memberlistResp.Members)

	// Usually replica should be equal to memberCnt. If it isn't, then
	// it means previous reconcile loop somehow interrupted right after
	// adding (replica < memberCnt) or removing (replica > memberCnt)
	// a member from the cluster. In that case, we shouldn't run health
	// check on the not yet started or already removed member.
	cnt := min(replica, memberCnt)

	lg.Info("health checking", "replica", replica, "len(members)", memberCnt)
	endpoints = endpoints[:cnt]

	healthInfos, err := clusterHealth(endpoints)
	if err != nil {
		return memberlistResp, nil, err
	}

	for _, healthInfo := range healthInfos {
		if !healthInfo.Health {
			// TODO: also update metrics?
			return memberlistResp, healthInfos, fmt.Errorf(healthInfo.String())
		}
		lg.Info(healthInfo.String())
	}

	return memberlistResp, healthInfos, nil
}

func clientEndpointsFromStatefulsets(sts *appsv1.StatefulSet) []string {
	var endpoints []string
	replica := int(*sts.Spec.Replicas)
	if replica > 0 {
		for i := 0; i < replica; i++ {
			endpoints = append(endpoints, clientEndpointForOrdinalIndex(sts, i))
		}
	}
	return endpoints
}

func clientEndpointForOrdinalIndex(sts *appsv1.StatefulSet, index int) string {
	return fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2379",
		sts.Name, index, sts.Name, sts.Namespace)
}

func peerEndpointForOrdinalIndex(ec *ecv1alpha1.EtcdCluster, index int) (string, string) {
	name := fmt.Sprintf("%s-%d", ec.Name, index)
	return name, fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:2380",
		ec.Name, index, ec.Name, ec.Namespace)
}

// enqueueEtcdCluster takes an EtcdCluster resource and converts it into a
// namespace/name string which is then put onto the work queue. This method
// should *not* be passed resources of any type other than EtcdCluster.
func (c *Controller) enqueueEtcdCluster(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.Add(key)
}

func newStatefulsets(ec *ecv1alpha1.EtcdCluster, replica int32) *appsv1.StatefulSet {
	labels := map[string]string{
		"app":        ec.Name,
		"controller": ec.Name,
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ec.Name,
			Namespace: ec.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(ec, ecv1alpha1.SchemeGroupVersion.WithKind("EtcdCluster")),
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replica,
			ServiceName: ec.Name,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "etcd",
							Command: []string{"/usr/local/bin/etcd"},
							Args: []string{
								"--name=$(POD_NAME)",
								"--listen-peer-urls=http://0.0.0.0:2380",   //TODO: only listen on 127.0.0.1 and host IP
								"--listen-client-urls=http://0.0.0.0:2379", //TODO: only listen on 127.0.0.1 and host IP
								fmt.Sprintf("--initial-advertise-peer-urls=http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2380", ec.Name),
								fmt.Sprintf("--advertise-client-urls=http://$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:2379", ec.Name),
							},
							Image: fmt.Sprintf("gcr.io/etcd-development/etcd:%s", ec.Spec.Version),
							Env: []corev1.EnvVar{
								{
									Name: "POD_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{
									Name: "POD_NAMESPACE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.namespace",
										},
									},
								},
							},
							EnvFrom: []corev1.EnvFromSource{
								{
									ConfigMapRef: &corev1.ConfigMapEnvSource{
										LocalObjectReference: corev1.LocalObjectReference{
											Name: configMapNameForEtcdCluster(ec),
										},
									},
								},
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          "client",
									ContainerPort: 2379,
								},
								{
									Name:          "peer",
									ContainerPort: 2380,
								},
							},
						},
					},
				},
			},
		},
	}
}

func configMapNameForEtcdCluster(ec *ecv1alpha1.EtcdCluster) string {
	return fmt.Sprintf("%s-state", ec.Name)
}

func (c *Controller) applyEtcdClusterState(ec *ecv1alpha1.EtcdCluster, replica int) error {
	cm := newEtcdClusterState(ec, replica)
	if _, err := c.kubeClientset.CoreV1().ConfigMaps(ec.Namespace).Get(context.TODO(), configMapNameForEtcdCluster(ec), metav1.GetOptions{}); err != nil {
		if errors.IsNotFound(err) {
			_, cerr := c.kubeClientset.CoreV1().ConfigMaps(ec.Namespace).Create(context.TODO(), cm, metav1.CreateOptions{})
			return cerr
		}
		return fmt.Errorf("cannot find ConfigMap for EtcdCluster %s: %w", ec.Name, err)
	}

	_, uerr := c.kubeClientset.CoreV1().ConfigMaps(ec.Namespace).Update(context.TODO(), cm, metav1.UpdateOptions{})
	return uerr
}

func newEtcdClusterState(ec *ecv1alpha1.EtcdCluster, replica int) *corev1.ConfigMap {
	// We always add members one by one, so the state is always
	// "existing" if replica > 1.
	state := "new"
	if replica > 1 {
		state = "existing"
	}

	var initialCluster []string
	for i := 0; i < replica; i++ {
		name, peerURL := peerEndpointForOrdinalIndex(ec, i)
		initialCluster = append(initialCluster, fmt.Sprintf("%s=%s", name, peerURL))
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapNameForEtcdCluster(ec),
			Namespace: ec.Namespace,
		},
		Data: map[string]string{
			"ETCD_INITIAL_CLUSTER_STATE": state,
			"ETCD_INITIAL_CLUSTER":       strings.Join(initialCluster, ","),
		},
	}
}

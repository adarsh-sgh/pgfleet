// EKS cluster for pgfleet: private-subnet managed nodes, EBS CSI driver (IRSA) with an
// expandable gp3 default StorageClass so PostgresInstance storage resizes work, then the CRDs,
// RBAC and the operator Deployment from ../../config.
import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as awsx from "@pulumi/awsx";
import * as eks from "@pulumi/eks";
import * as k8s from "@pulumi/kubernetes";

const cfg = new pulumi.Config("pgfleet");
const image = cfg.get("image") ?? "ghcr.io/adarsh-sgh/pgfleet:latest";
const nodeCount = cfg.getNumber("nodeCount") ?? 3;
const instanceType = cfg.get("instanceType") ?? "m7i.large";

const vpc = new awsx.ec2.Vpc("pgfleet", {
  numberOfAvailabilityZones: 3,
  natGateways: { strategy: awsx.ec2.NatGatewayStrategy.Single },
  subnetSpecs: [
    { type: awsx.ec2.SubnetType.Public, tags: { "kubernetes.io/role/elb": "1" } },
    { type: awsx.ec2.SubnetType.Private, tags: { "kubernetes.io/role/internal-elb": "1" } },
  ],
});

const cluster = new eks.Cluster("pgfleet", {
  vpcId: vpc.vpcId,
  publicSubnetIds: vpc.publicSubnetIds,
  privateSubnetIds: vpc.privateSubnetIds,
  authenticationMode: eks.AuthenticationMode.Api,
  createOidcProvider: true,
  skipDefaultNodeGroup: true,
});

const nodeRole = new aws.iam.Role("pgfleet-nodes", {
  assumeRolePolicy: aws.iam.assumeRolePolicyForPrincipal({ Service: "ec2.amazonaws.com" }),
  managedPolicyArns: [
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
  ],
});

const nodes = new eks.ManagedNodeGroup("pgfleet", {
  cluster,
  nodeRole,
  subnetIds: vpc.privateSubnetIds,
  instanceTypes: [instanceType],
  scalingConfig: { desiredSize: nodeCount, minSize: nodeCount, maxSize: nodeCount * 2 },
});

// IRSA: only the EBS CSI controller's service account may assume this role.
const oidcHost = cluster.oidcProviderUrl.apply((u) => u.replace(/^https:\/\//, ""));
const ebsRole = new aws.iam.Role("pgfleet-ebs-csi", {
  assumeRolePolicy: pulumi.all([cluster.oidcProviderArn, oidcHost]).apply(([arn, host]) =>
    JSON.stringify({
      Version: "2012-10-17",
      Statement: [{
        Effect: "Allow",
        Principal: { Federated: arn },
        Action: "sts:AssumeRoleWithWebIdentity",
        Condition: { StringEquals: {
          [`${host}:sub`]: "system:serviceaccount:kube-system:ebs-csi-controller-sa",
          [`${host}:aud`]: "sts.amazonaws.com",
        } },
      }],
    })),
  managedPolicyArns: ["arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"],
});

const ebsCsi = new aws.eks.Addon("ebs-csi", {
  clusterName: cluster.eksCluster.name,
  addonName: "aws-ebs-csi-driver",
  serviceAccountRoleArn: ebsRole.arn,
}, { dependsOn: [nodes] });

const provider = new k8s.Provider("pgfleet", { kubeconfig: cluster.kubeconfigJson });

// allowVolumeExpansion is what lets the operator grow a PVC in place.
new k8s.storage.v1.StorageClass("gp3", {
  metadata: { name: "gp3", annotations: { "storageclass.kubernetes.io/is-default-class": "true" } },
  provisioner: "ebs.csi.aws.com",
  allowVolumeExpansion: true,
  volumeBindingMode: "WaitForFirstConsumer",
  parameters: { type: "gp3", encrypted: "true" },
}, { provider, dependsOn: [ebsCsi] });

const crds = new k8s.yaml.ConfigGroup("pgfleet-crds", { files: ["../../config/crd/*.yaml"] }, { provider });

new k8s.yaml.ConfigGroup("pgfleet-operator", {
  files: ["../../config/rbac/role.yaml", "../../config/manager/manager.yaml"],
  transformations: [(obj: any) => {
    if (obj.kind === "Deployment") {
      obj.spec.template.spec.containers[0].image = image;
    }
  }],
}, { provider, dependsOn: [crds] });

export const clusterName = cluster.eksCluster.name;
export const kubeconfig = pulumi.secret(cluster.kubeconfigJson);

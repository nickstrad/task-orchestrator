package task

import (
	"context"
	"io"
	"log"
	"math"
	"os"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

type Docker struct {
	Client *client.Client
	Config Config
}

type DockerResult struct {
	Error       *TaskError
	Action      string
	ContainerId string
	Result      string
}

type DockerInspectResponse struct {
	Error     error
	Container *client.ContainerInspectResult
}

func NewDocker(config Config) *Docker {
	dockerClient, _ := client.New(client.FromEnv)
	return &Docker{
		Client: dockerClient,
		Config: config,
	}
}

func NewConfig(t Task) Config {
	config := Config{
		Name:          t.Name,
		Image:         t.Image,
		Memory:        int64(t.Memory),
		Disk:          int64(t.Disk),
		RestartPolicy: t.RestartPolicy,
	}

	if t.ExposedPorts != nil {
		config.ExposedPorts = t.ExposedPorts
	}

	return config
}

func NewDockerResult(err *TaskError, action, containerId, result string) DockerResult {
	d := DockerResult{
		Action:      action,
		ContainerId: containerId,
		Result:      result,
	}

	if err != nil {
		d.Error = err
	}

	return d
}

func (d *Docker) Run() DockerResult {
	ctx := context.Background()
	reader, err := d.Client.ImagePull(
		ctx, d.Config.Image, client.ImagePullOptions{},
	)

	if err != nil {
		taskError := WrapError(err, "Error pulling image %s: %v\n", d.Config.Image, err)
		return NewDockerResult(taskError, "", "", "")
	}

	io.Copy(os.Stdout, reader)

	restartPolicy := container.RestartPolicy{
		Name: container.RestartPolicyMode(d.Config.RestartPolicy),
	}
	resources := container.Resources{
		Memory:   d.Config.Memory,
		NanoCPUs: int64(d.Config.Cpu * math.Pow(10, 9)),
	}

	containerConfig := container.Config{
		Image:        d.Config.Image,
		Tty:          false,
		Env:          d.Config.Env,
		ExposedPorts: d.Config.ExposedPorts,
	}

	hostConfig := container.HostConfig{
		RestartPolicy:   restartPolicy,
		Resources:       resources,
		PublishAllPorts: true,
	}

	containerCreateOptions := client.ContainerCreateOptions{
		Config:     &containerConfig,
		HostConfig: &hostConfig,
		Name:       d.Config.Name,
	}

	containerCreateResp, err := d.Client.ContainerCreate(ctx, containerCreateOptions)
	if err != nil {
		taskError := WrapError(err, "Error creating container using image %s: %v\n", d.Config.Image, err)
		return NewDockerResult(taskError, "", "", "")
	}

	containerStartOptions := client.ContainerStartOptions{}
	_, err = d.Client.ContainerStart(ctx, containerCreateResp.ID, containerStartOptions)
	if err != nil {
		taskError := WrapError(err, "Error starting container with id %s: %v\n", containerCreateResp.ID, err)
		return NewDockerResult(taskError, "", "", "")
	}

	d.Config.ContainerID = containerCreateResp.ID

	containerLogOptions := client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	}
	out, err := d.Client.ContainerLogs(ctx, containerCreateResp.ID, containerLogOptions)
	if err != nil {
		taskError := WrapError(err, "Error getting logs from container with id %s: %v\n", containerCreateResp.ID, err)
		return NewDockerResult(taskError, "", "", "")
	}

	stdcopy.StdCopy(os.Stdout, os.Stderr, out)
	return NewDockerResult(nil, "start", containerCreateResp.ID, "success")
}

func (d *Docker) Stop(id string) DockerResult {
	log.Printf("Attempting to stop container %v", id)
	ctx := context.Background()

	containerStopOptions := client.ContainerStopOptions{}
	_, err := d.Client.ContainerStop(ctx, id, containerStopOptions)
	if err != nil {
		taskError := WrapError(err, "Error stopping container with id %s: %v\n", id, err)
		return NewDockerResult(taskError, "", "", "")
	}

	containerRemoveOptions := client.ContainerRemoveOptions{}

	_, err = d.Client.ContainerRemove(ctx, id, containerRemoveOptions)
	if err != nil {
		taskError := WrapError(err, "Error removing container with id %s: %v\n", id, err)
		return NewDockerResult(taskError, "", "", "")
	}

	return NewDockerResult(nil, "stop", id, "success")
}

func (d *Docker) Inspect(containerID string) DockerInspectResponse {
	ctx := context.Background()
	containerInspectOptions := client.ContainerInspectOptions{}
	resp, err := d.Client.ContainerInspect(ctx, containerID, containerInspectOptions)

	if err != nil {
		log.Printf("Error inspecting container: %s\n", err)
		return DockerInspectResponse{Error: err}
	}

	return DockerInspectResponse{Container: &resp}
}

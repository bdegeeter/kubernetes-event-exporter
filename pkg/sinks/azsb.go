package sinks

import (
	"context"

	azsb "github.com/Azure/azure-service-bus-go"
	"github.com/opsgenie/kubernetes-event-exporter/pkg/kube"
	"github.com/rs/zerolog/log"
)

type AzSbConfig struct {
	Endpoint     string `yaml:"endpoint"`
	Topic        string `yaml:"topic"`
	Subscription string `yaml:"subscription"`
}

type AzSbSink struct {
	cfg          *AzSbConfig
	topic        *azsb.Topic
	namespace    *azsb.Namespace
	subscription *azsb.Subscription
}

func NewAzSbSink(cfg *AzSbConfig) (Sink, error) {
	log.Info().Msg("new azure service bus sink")
	//ctx := context.Background()
	ns, err := azsb.NewNamespace(azsb.NamespaceWithConnectionString(cfg.Endpoint))
	if err != nil {
		return nil, err
	}
	topic, err := ns.NewTopic(cfg.Topic)
	if err != nil {
		return nil, err
	}
	sub, err := topic.NewSubscription(cfg.Subscription)
	if err != nil {
		return nil, err
	}
	return &AzSbSink{
		cfg:          cfg,
		namespace:    ns,
		topic:        topic,
		subscription: sub,
	}, nil
}

func (azs *AzSbSink) Send(ctx context.Context, ev *kube.EnhancedEvent) error {
	msg := &azsb.Message{
		Data: ev.ToJSON(),
	}
	azs.topic.Send(ctx, msg)
	return nil
}
func (azs *AzSbSink) Close() {
	azs.topic.Close(context.TODO())
}

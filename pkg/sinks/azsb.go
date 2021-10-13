package sinks

import (
	"context"
	"encoding/json"

	azsb "github.com/Azure/azure-service-bus-go"
	"github.com/opsgenie/kubernetes-event-exporter/pkg/kube"
	"github.com/rs/zerolog/log"
)

type AzSbConfig struct {
	Endpoint string `yaml:"endpoint"`
	Topic    string `yaml:"topic"`
	//Subscription string                 `yaml:"subscription"`
	Layout map[string]interface{} `yaml:"layout"`
}

type AzSbSink struct {
	cfg    *AzSbConfig
	layout map[string]interface{}
}

func NewAzSbSink(cfg *AzSbConfig) (Sink, error) {
	log.Info().Msg("new azure service bus sink")
	//ctx := context.Background()
	// sub, err := topic.NewSubscription(cfg.Subscription)
	// if err != nil {
	// 	return nil, err
	// }
	return &AzSbSink{
		cfg:    cfg,
		layout: cfg.Layout,
	}, nil
}

func (azs *AzSbSink) Send(ctx context.Context, ev *kube.EnhancedEvent) error {
	ns, err := azsb.NewNamespace(azsb.NamespaceWithConnectionString(azs.cfg.Endpoint))
	if err != nil {
		return err
	}
	topic, err := ns.NewTopic(azs.cfg.Topic)
	if err != nil {
		return err
	}
	defer topic.Close(ctx)
	msg := &azsb.Message{}
	if azs.layout == nil {
		msg.Data = ev.ToJSON()
	} else {
		res, err := convertLayoutTemplate(azs.layout, ev)
		if err != nil {
			log.Debug().Err(err).Msg("Failed to convert event to layout template")
			return err
		}
		str, err := json.Marshal(res)
		if err != nil {
			log.Debug().Err(err).Msg("Failed to marshal message data")
			return err
		}
		msg.Data = str
	}
	err = topic.Send(ctx, msg)
	if err != nil {
		log.Debug().Err(err).Msg("Error sending message on service bus")
		return err
	}
	log.Debug().Msg("Message successfully sent on azure service bus")
	return nil
}
func (azs *AzSbSink) Close() {
	//azs.topic.Close(context.TODO())
}

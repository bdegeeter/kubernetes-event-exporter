package sinks

import (
	"context"
	"encoding/json"

	azsb "github.com/Azure/azure-service-bus-go"
	"github.com/opsgenie/kubernetes-event-exporter/pkg/kube"
	"github.com/rs/zerolog/log"
)

type AzSbConfig struct {
	Endpoint string                 `yaml:"endpoint"`
	Topic    string                 `yaml:"topic"`
	Layout   map[string]interface{} `yaml:"layout"`
}

type AzSbSink struct {
	cfg    *AzSbConfig
	layout map[string]interface{}
}

func NewAzSbSink(cfg *AzSbConfig) (Sink, error) {
	log.Info().Msg("new azure service bus sink")
	return &AzSbSink{
		cfg:    cfg,
		layout: cfg.Layout,
	}, nil
}

func (azs *AzSbSink) Send(ctx context.Context, ev *kube.EnhancedEvent) error {
	log.Debug().Msg("Attempting to connect using connection string")
	ns, err := azsb.NewNamespace(azsb.NamespaceWithConnectionString(azs.cfg.Endpoint))
	if err != nil {
		log.Debug().Err(err).Msg("Unable to connect to service bus with connection string, attempting environment binding")
		ns, err = azsb.NewNamespace(azsb.NamespaceWithEnvironmentBinding(azs.cfg.Endpoint))
		if err != nil {
			log.Debug().Err(err).Msg("Unable to connect with environment binding")
			return err
		}
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
			return err
		}
		str, err := json.Marshal(res)
		if err != nil {
			return err
		}
		msg.Data = str
	}
	err = topic.Send(ctx, msg)
	if err != nil {
		return err
	}
	return nil
}
func (azs *AzSbSink) Close() {
	//azs.topic.Close(context.TODO())
}

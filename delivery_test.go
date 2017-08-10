// build +integration
package segment

import (
	"log"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
)

func TestConnect(t *testing.T) {
	cfg := aws.NewConfig().WithRegion("us-west-2").WithEndpoint("http://localhost:4573")
	sess, err := session.NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	d := &Delivery{
		Logger:     log.New(os.Stderr, "", log.LstdFlags),
		fh:         firehose.New(sess, cfg),
		streamName: "test-stream",
	}
	if err := d.Connect(); err != nil {
		t.Error(err)
	}
}

package main

import (
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/gtfierro/bwquery/api"
	messages "github.com/gtfierro/durandal/archiver"
	"github.com/pkg/errors"
	bw "gopkg.in/immesys/bw2bind.v5"
	"gopkg.in/readline.v1"
	"os"
	"os/user"
	"time"
)

func doQuery(c *cli.Context) error {
	client := bw.ConnectOrExit("")
	vk := client.SetEntityFileOrExit(c.String("entity"))
	client.OverrideAutoChainTo(true)
	API := api.NewAPI(client, vk, c.String("archiver"))
	return API.Query(c.String("query"))
}

func doIQuery(c *cli.Context) error {
	client := bw.ConnectOrExit("")
	vk := client.SetEntityFileOrExit(c.String("entity"))
	client.OverrideAutoChainTo(true)
	API := api.NewAPI(client, vk, c.String("archiver"))

	res, err := client.Query(&bw.QueryParams{
		URI: c.String("archiver") + "/s.giles/!meta/lastalive",
	})
	if err != nil {
		return err
	}
	for msg := range res {
		var md map[string]interface{}
		po := msg.GetOnePODF(bw.PODFMaskSMetadata)
		if err := po.(bw.MsgPackPayloadObject).ValueInto(&md); err != nil {
			fmt.Println(errors.Wrap(err, "Could not decode lastalive time"))
		} else {
			//2016-09-16 10:41:40.818797445 -0700 PDT
			lastalive, err := time.Parse("2006-01-02 15:04:05 -0700 MST", md["val"].(string))
			if err != nil {
				fmt.Println(errors.Wrap(err, "Could not decode lastalive time"))
			}
			ago := time.Since(lastalive)
			fmt.Printf("Archiver at %s last alive at %v (%v ago)", c.String("archiver"), lastalive, ago)
		}
	}

	currentUser, err := user.Current()
	if err != nil {
		return err
	}

	completer := readline.NewPrefixCompleter(
		readline.PcItem("select",
			readline.PcItem("data",
				readline.PcItem("in"),
				readline.PcItem("before"),
				readline.PcItem("after"),
			),
			readline.PcItem("Metadata/"),
			readline.PcItem("distinct",
				readline.PcItem("Metadata/"),
				readline.PcItem("uuid/"),
			),
			readline.PcItem("uuid"),
		),
	)

	rl, err := readline.NewEx(&readline.Config{
		Prompt:       "(bwquery)>",
		AutoComplete: completer,
		HistoryFile:  currentUser.HomeDir + "/.bwquery",
	})
	if err != nil {
		return err
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err != nil {
			fmt.Println(err)
			break
		}
		API.Query(line)
	}
	return nil
}

func doSubscribe(c *cli.Context) error {
	client := bw.ConnectOrExit("")
	vk := client.SetEntityFileOrExit(c.String("entity"))
	client.OverrideAutoChainTo(true)
	API := api.NewAPI(client, vk, c.String("archiver"))
	return API.SubscribeData(c.String("query"), dump)
}

func dump(ts messages.QueryTimeseriesResult) {
	if len(ts.Data) > 0 {
		ts.Dump()
	}
}

func main() {
	app := cli.NewApp()
	app.Name = "bwquery"
	app.Version = "0.0.4"

	app.Commands = []cli.Command{
		{
			Name:   "query",
			Usage:  "Evaluate query",
			Action: doQuery,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "entity,e",
					Value:  "",
					Usage:  "The entity to use",
					EnvVar: "BW2_DEFAULT_ENTITY",
				},
				cli.StringFlag{
					Name:  "archiver,a",
					Value: "gabe.ns",
					Usage: "REQUIRED. The URI you want to archive",
				},
				cli.StringFlag{
					Name:  "query,q",
					Value: "",
					Usage: "Giles query string",
				},
			},
		},
		{
			Name:   "iquery",
			Usage:  "Evaluate query interactively",
			Action: doIQuery,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "entity,e",
					Value:  "",
					Usage:  "The entity to use",
					EnvVar: "BW2_DEFAULT_ENTITY",
				},
				cli.StringFlag{
					Name:  "archiver,a",
					Value: "gabe.ns",
					Usage: "REQUIRED. The URI you want to archive",
				},
			},
		},
		{
			Name:   "subscribe",
			Usage:  "Subscribe to Giles",
			Action: doSubscribe,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:   "entity,e",
					Value:  "",
					Usage:  "The entity to use",
					EnvVar: "BW2_DEFAULT_ENTITY",
				},
				cli.StringFlag{
					Name:  "archiver,a",
					Value: "gabe.ns",
					Usage: "REQUIRED. The URI you want to archive",
				},
				cli.StringFlag{
					Name:  "query,q",
					Value: "",
					Usage: "Giles query string",
				},
			},
		},
	}
	app.Run(os.Args)
}

package api

import (
	"fmt"
	messages "github.com/gtfierro/durandal/archiver"
	bw "gopkg.in/immesys/bw2bind.v5"
	"math/rand"
	"strings"
	"sync"
	"time"
)

const GilesQueryChangedRangesPIDString = "2.0.8.8"

var GilesQueryChangedRangesPID = bw.FromDotForm(GilesQueryChangedRangesPIDString)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type API struct {
	client *bw.BW2Client
	vk     string
	uri    string
}

// Create a new API isntance w/ the given client and VerifyingKey.
// The verifying key is returned by any of the BW2Client.SetEntity* calls
// URI should be the base of the giles service
func NewAPI(client *bw.BW2Client, vk string, uri string) *API {
	return &API{
		client: client,
		vk:     vk,
		uri:    strings.TrimSuffix(uri, "/") + "/s.giles/_/i.archiver",
	}
}

func (api *API) Query(query string) error {
	if len(query) == 0 {
		return nil
	}
	var wg sync.WaitGroup
	nonce := rand.Uint32()
	msg := messages.KeyValueQuery{
		Query: query,
		Nonce: nonce,
	}
	wg.Add(1)
	fmt.Printf("Subscribe to %v\n", api.uri+fmt.Sprintf("/signal/%s,queries", api.vk[:len(api.vk)-1]))
	c, err := api.client.Subscribe(&bw.SubscribeParams{
		URI: api.uri + fmt.Sprintf("/signal/%s,queries", api.vk[:len(api.vk)-1]),
	})
	if err != nil {
		return err
	}
	go func() {
		for msg := range c {
			var isMyResponse bool = false

			// check for error
			found, err := GetError(nonce, msg)
			isMyResponse = isMyResponse || found
			if found {
				fmt.Println(err)
				wg.Done()
				break
			}

			// check for metadata
			found, metadata, err := GetMetadata(nonce, msg)
			isMyResponse = isMyResponse || found
			if err == nil && found {
				fmt.Println(metadata.Dump())
			} else if found && err != nil {
				fmt.Println(err)
			}

			// check for timeseries
			found, timeseries, err := GetTimeseries(nonce, msg)
			isMyResponse = isMyResponse || found
			if err == nil && found {
				fmt.Println(timeseries.Dump())
			} else if found && err != nil {
				fmt.Println(err)
			}

			// check for changed
			found, changed, err := GetChanged(nonce, msg)
			isMyResponse = isMyResponse || found
			if err == nil && found {
				fmt.Println(changed.Dump())
			} else if found && err != nil {
				fmt.Println(err)
			}

			if isMyResponse {
				wg.Done()
			}
		}
	}()
	err = api.client.Publish(&bw.PublishParams{
		URI:            api.uri + "/slot/query",
		PayloadObjects: []bw.PayloadObject{msg.ToMsgPackBW()},
	})
	fmt.Printf("Publish to %v\n", api.uri+"/slot/query")
	wg.Wait()
	return nil
}

func (api *API) SubscribeData(query string, cb func(ts messages.QueryTimeseriesResult)) error {
	nonce := rand.Uint32()
	msg := messages.KeyValueQuery{
		Query: query,
		Nonce: nonce,
	}
	fmt.Printf("Subscribe to %v\n", api.uri+fmt.Sprintf("/signal/%s,all", api.vk[:len(api.vk)-1]))
	c, err := api.client.Subscribe(&bw.SubscribeParams{
		URI: api.uri + fmt.Sprintf("/signal/%s,all", api.vk[:len(api.vk)-1]),
	})
	if err != nil {
		return err
	}
	err = api.client.Publish(&bw.PublishParams{
		URI:            api.uri + "/slot/subscribe",
		PayloadObjects: []bw.PayloadObject{msg.ToMsgPackBW()},
	})
	fmt.Printf("Publish to %v\n", api.uri+"/slot/subscribe")
	for msg := range c {
		found, timeseries, err := GetTimeseries(nonce, msg)
		if err == nil && found {
			cb(timeseries)
		} else if err != nil {
			return err
		}
	}
	return nil
}

// Extracts QueryError from Giles response. Returns false if no related message was found
func GetError(nonce uint32, msg *bw.SimpleMessage) (bool, error) {
	var (
		po         bw.PayloadObject
		queryError messages.QueryError
	)
	if po = msg.GetOnePODF(bw.PODFGilesQueryError); po != nil {
		if err := po.(bw.MsgPackPayloadObject).ValueInto(&queryError); err != nil {
			return false, err
		}
		if queryError.Nonce != nonce {
			return false, nil
		}
		return true, nil
	}
	return false, nil
}

// Extracts Metadata from Giles response. Returns false if no related message was found
func GetMetadata(nonce uint32, msg *bw.SimpleMessage) (bool, messages.QueryMetadataResult, error) {
	var (
		po              bw.PayloadObject
		metadataResults messages.QueryMetadataResult
	)
	if po = msg.GetOnePODF(bw.PODFGilesMetadataResponse); po != nil {
		if err := po.(bw.MsgPackPayloadObject).ValueInto(&metadataResults); err != nil {
			return false, metadataResults, err
		}
		if metadataResults.Nonce != nonce {
			return false, metadataResults, nil
		}
		return true, metadataResults, nil
	}
	return false, metadataResults, nil
}

// Extracts Timeseries from Giles response. Returns false if no related message was found
func GetTimeseries(nonce uint32, msg *bw.SimpleMessage) (bool, messages.QueryTimeseriesResult, error) {
	var (
		po                bw.PayloadObject
		timeseriesResults messages.QueryTimeseriesResult
	)
	if po = msg.GetOnePODF(bw.PODFGilesTimeseriesResponse); po != nil {
		if err := po.(bw.MsgPackPayloadObject).ValueInto(&timeseriesResults); err != nil {
			return false, timeseriesResults, err
		}
		if timeseriesResults.Nonce != nonce {
			return false, timeseriesResults, nil
		}
		return true, timeseriesResults, nil
	}
	return false, timeseriesResults, nil
}

// Extracts Timeseries from Giles response. Returns false if no related message was found
func GetChanged(nonce uint32, msg *bw.SimpleMessage) (bool, messages.QueryChangedResult, error) {
	var (
		po             bw.PayloadObject
		changedResults messages.QueryChangedResult
	)
	if po = msg.GetOnePODF(GilesQueryChangedRangesPIDString); po != nil {
		if err := po.(bw.MsgPackPayloadObject).ValueInto(&changedResults); err != nil {
			return false, changedResults, err
		}
		if changedResults.Nonce != nonce {
			return false, changedResults, nil
		}
		return true, changedResults, nil
	}
	return false, changedResults, nil
}

//func GetChangedRanges(nonce uint32, msg *bw.SimpleMessage) (bool, messages.Query

package leveltsd

import (
	"errors"
	"net/http"
)

type ReaderService struct {
	federator *levelfederator
}

type Nodename struct {
	Node string
}

type Nodelist struct {
	Nodes []string
}

type Bool struct {
	Yes bool
}

type RangeQuery struct {
	Node  string
	Start uint64
	End   uint64
}

type RangeResult struct {
	Data []Datapoint
}

func (this *ReaderService) GetChildNodes(r *http.Request, parent *Nodename, children *Nodelist) error {
	retval := this.federator.idx.listChildern(parent.Node)
	children.Nodes = retval
	return nil
}

func (this *ReaderService) IsNodeLeaf(r *http.Request, node *Nodename, response *Bool) error {
	_, response.Yes = this.federator.idx.getMetric(node.Node, false)
	return nil
}

func (this *ReaderService) GetRangeData(r *http.Request, query *RangeQuery, response *RangeResult) error {
	if key, ok := this.federator.getMetric(query.Node); !ok {
		return errors.New("key not found: " + query.Node)
	} else {
		response.Data = this.federator.dataScan(key, query.Start, query.End)
		return nil
	}
}

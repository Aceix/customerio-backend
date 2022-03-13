package datastore

import (
	"errors"
	"strconv"
	"time"

	"github.com/customerio/homework/serve"
	"github.com/customerio/homework/stream"
	"github.com/labstack/gommon/log"
)

type Datastore struct {
	customers []*serve.Customer

	// link the customer id to the position in the 'customers' slice
	customerLinks map[int]int

	// map to keep track of processed events
	processedEventsMap map[string]bool
}

// New creates an in-memory instance of the datastore from an input channel of records
func New(inputChannel <-chan *stream.Record) (serve.Datastore, error) {
	store := Datastore{
		customers:          make([]*serve.Customer, 0),
		customerLinks:      make(map[int]int),
		processedEventsMap: make(map[string]bool),
	}

	for record := range inputChannel {
		exists, err := isUserRegistered(&store, record.UserID)
		if err != nil {
			log.Errorf("Failed to check if user is registered: %s: %v", record.UserID, err)
			continue
		}

		if !exists {
			// if user is not registered, create new customer
			_, err = registerCustomerFromRecord(
				&store,
				record,
			)
			if err != nil {
				log.Errorf("Failed to register customer: %v", err)
			}
		}

		// process the event against the registered customer
		if err = processRecord(&store, record); err != nil {
			log.Errorf("Failed to process record: %v", err)
			return nil, err
		}
	}

	return &store, nil
}

func isUserRegistered(ds *Datastore, customerId string) (bool, error) {
	if ds == nil {
		return false, errors.New("datastore is nil")
	}

	userId, err := strconv.Atoi(customerId)
	if err != nil {
		log.Errorf("Invalid user ID: %s: %v", customerId, err)
		return false, err
	}

	if _, exists := ds.customerLinks[userId]; !exists {
		return false, nil
	}

	return true, nil
}

func registerCustomerFromRecord(ds *Datastore, record *stream.Record) (*serve.Customer, error) {
	if ds == nil {
		return nil, errors.New("datastore is nil")
	}

	userId, err := strconv.Atoi(record.UserID)
	if err != nil {
		log.Errorf("Invalid user ID: %s: %v", record.UserID, err)
		return nil, err
	}

	newCustomer := &serve.Customer{
		ID:          userId,
		Attributes:  make(map[string]string),
		Events:      make(map[string]int),
		LastUpdated: int(record.Timestamp),
	}

	ds.customers = append(ds.customers, newCustomer)
	ds.customerLinks[userId] = len(ds.customers) - 1

	return newCustomer, nil
}

func registerCustomer(ds *Datastore, id int, attributes map[string]string) (*serve.Customer, error) {
	if ds == nil {
		return nil, errors.New("datastore is nil")
	}

	newCustomer := &serve.Customer{
		ID:          id,
		Attributes:  make(map[string]string),
		Events:      make(map[string]int),
		LastUpdated: int(time.Now().Unix()),
	}

	for attrName, attrVal := range attributes {
		newCustomer.Attributes[attrName] = attrVal
	}

	ds.customers = append(ds.customers, newCustomer)
	ds.customerLinks[id] = len(ds.customers) - 1

	return newCustomer, nil
}

func processRecord(ds *Datastore, record *stream.Record) error {
	if ds == nil {
		return errors.New("datastore is nil")
	}

	customerId, err := strconv.Atoi(record.UserID)
	if err != nil {
		log.Errorf("Invalid user ID: %s: %v", record.UserID, err)
		return err
	}

	customer := ds.customers[ds.customerLinks[customerId]]

	switch record.Type {
	case "event":
		// skip processing if already processed
		if _, exists := ds.processedEventsMap[record.ID]; exists {
			return nil
		}

		// if event does not exist, create and set count to 1. otherwise just increase count
		if _, exists := customer.Events[record.Name]; !exists {
			customer.Events[record.Name] = 1
		} else {
			customer.Events[record.Name]++
		}

		// mark event as processed
		ds.processedEventsMap[record.ID] = true
	case "attributes":
		if int(record.Timestamp) >= customer.LastUpdated {
			// set each attribute as-is
			for attrName, attrVal := range record.Data {
				customer.Attributes[attrName] = attrVal
			}
		}
	}

	// check and update customer last updated time
	if customer.LastUpdated < int(record.Timestamp) {
		customer.LastUpdated = int(record.Timestamp)
	}

	return nil
}

func fixLinks(ds *Datastore) error {
	if ds == nil {
		return errors.New("datastore is nil")
	}

	// // loop through customers and fix links
	// for i, customer := range ds.customers {
	// 	ds.customerLinks[customer.ID] = i
	// }

	// loop through customers and fix links (2-way to half time)
	for i, j := 0, len(ds.customers)-1; i < j; i, j = i+1, j-1 {
		ds.customerLinks[ds.customers[i].ID], ds.customerLinks[ds.customers[j].ID] = i, j
	}

	return nil
}

// Get retrieves a customer's data by ID
func (ds *Datastore) Get(id int) (*serve.Customer, error) {
	pos, exists := ds.customerLinks[id]
	if !exists {
		return nil, errors.New("customer not found")
	}

	customer := ds.customers[pos]
	return customer, nil
}

// List retrieves all customers in the datastore
func (ds *Datastore) List(page, count int) ([]*serve.Customer, error) {
	return ds.customers, nil
}

// Create adds a new customer to the datastore
func (ds *Datastore) Create(id int, attributes map[string]string) (*serve.Customer, error) {
	if _, exists := ds.customerLinks[id]; exists {
		return nil, errors.New("customer already exists")
	}

	return registerCustomer(ds, id, attributes)
}

// Update updates a customer's attribute data
func (ds *Datastore) Update(id int, attributes map[string]string) (*serve.Customer, error) {
	pos, exists := ds.customerLinks[id]
	if !exists {
		return nil, errors.New("customer does not exist")
	}

	customer := ds.customers[pos]
	for attrName, attrVal := range attributes {
		customer.Attributes[attrName] = attrVal
	}

	customer.LastUpdated = int(time.Now().Unix())

	return customer, nil
}

// Delete removes a customer from the datastore
func (ds *Datastore) Delete(id int) error {
	pos, exists := ds.customerLinks[id]
	if !exists {
		return errors.New("customer not found")
	}

	// delete link
	delete(ds.customerLinks, id)

	// delete customer
	ds.customers[pos] = ds.customers[len(ds.customers)-1]
	ds.customers = ds.customers[:len(ds.customers)-1]

	// recalculate links
	return fixLinks(ds)
}

// TotalCustomers returns the total number of customers in the datastore
func (ds *Datastore) TotalCustomers() (int, error) {
	return len(ds.customers), nil
}

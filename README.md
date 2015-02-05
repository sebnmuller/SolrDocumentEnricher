## Solr Document Enricher

### A bit of Solr background
Solr ships with all the tools and features necessary for an advanced search solution. These include the oft overlooked update request processors. They operate at the document level i.e. prior to individual field tokenisation and allow you to clean, modify and/or enrich incoming documents. Processing options include language identification, duplicate detection and HTML markup handling. Create a chain of them and you have a true document processing pipeline.

### What problem does this update processor solve?
Some data sources contain documents with content fields that are dynamically populated by content from other documents. For a query to retrieve these kinds of documents, the external content needs to be retrieved and added during index time.

### How does this update processor perform its purpose?
During index time, the processor scans incoming documents for a predefined field, topicRef for example, with a reference to one or more documents already present in the index. The referenced document(s) either contains a subsequent reference field or content that we want to add to the incoming document.

![Solr Document Enricher](http://blog.comperiosearch.com/wp-content/uploads/2014/10/docProcess.png)

The processor retrieves any referenced documents, traverses a tree of subsequently referenced documents if necessary, and then maps the eventual leaf documents’ specified content fields to additional new fields in the incoming document.



### Documentation
In progress.

### Authors and Contributors
@sebnmuller

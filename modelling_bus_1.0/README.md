
/**************************************************************************************
 *
 * The modelling bus
 *
 **************************************************************************************

- Order of model updates
- A posting agent always start with a state update


 The modelling bus provides an infrastructure to let different agents exchange and
 manipulate (digital) artefacts related to a model-driven engineering workflow.

 Examples of such agents include:
 - graphical modelling front-end
 - code generators, e.g. from ER to SQL,
 - model transformers/synchronisers, e.g. DEMO to BPMN and back
 - a speech to text converter to enable spoken inputs
 - a coordinating agent, which coordinates the overall process
 - etc

 The artefacts shared on the bus can include:
 - Models (represented as JSON records)
 - Files (e.g. audio, images, models in an external format, office documents, etc)
 - Transactions (requests + acknowledge, and reports + acknowledge) between agents
   (used to coordinate the work "around" the bus
 - Observations of sensors and/or agents

 Per artefact kind, there are different rules regarding their behaviour when they
 are received by an agent:
 - Models are persistent. They may be updated, but multiple agents can read them.
 - Files are persistent as well.
 - Requests and reports are not persistent. Once read by their intended recipient, 
   they are
   removed (by the recipient) from the bus

EDITING



 A key assumption is that among the the agents connected to the bus, there is one
 coordinator. This is also why there is no general "group" or "all" request/report.
 The coordinator needs to distribute requests to each agent it would need to act.
 Note: any agent can share an observation.

 ... one coordinator of the work. So, also no group requests/reports

 Topics

 Simple ... FTP and MQTT

 So ... the topics ...

 The topical structure ... MQTT topics, and paths on the FTP server


Topics on the bus. Some pertaining to models. Some to messages. Some to files.

MQTT topics will be reflected in the path as well.


As the architecture of the modelling bus, as a concept in itself, may evolve, it
has a version (modellingBusVersion), which will be included in the topic

The modelling bus involves experiments with different set up of modelling
infrastructures. Each experiment has an identifier, which will also be included in the
topic/path

Models are identified by a unique identifier, which will also be included in
	Models are represented as JSON's with a structured that is determined by the 
	meta-model
	of the used modelling language.
	The mapping from the meta-model to the actual JSON structure might also have 
	variations.
	Therefore
   For models:
- MQTT (r=1): ~/models/<model id>/<json version>/{s,u,c}/"{ timestamp, link }"
- FTP: ~/models/<model id>/<json version>/{s,u,c}/<timestamp>.json

Files:
- FTP: ~/files/<poster id>/<format>/<timestamp>.extension

Messages:
- MQTT (r=0): ~/messages/<sender id>/<receiver id>/json/<json version>/{" json "}

 *************************************************************************************/


/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

-- schema for h2 engine.

CREATE TABLE framework_id(
  id INT PRIMARY KEY,
  framework_id VARCHAR NOT NULL,

  UNIQUE(framework_id)
);

CREATE TABLE job_keys(
  id INT IDENTITY,
  role VARCHAR NOT NULL,
  environment VARCHAR NOT NULL,
  name VARCHAR NOT NULL,

  UNIQUE(role, environment, name)
);

CREATE TABLE locks(
  id INT IDENTITY,
  job_key_id INT NOT NULL REFERENCES job_keys(id),
  token VARCHAR NOT NULL,
  user VARCHAR NOT NULL,
  timestampMs BIGINT NOT NULL,
  message VARCHAR,

  UNIQUE(job_key_id)
);

CREATE TABLE quotas(
  id INT IDENTITY,
  role VARCHAR NOT NULL,
  num_cpus FLOAT NOT NULL,
  ram_mb INT NOT NULL,
  disk_mb INT NOT NULL,

  UNIQUE(role)
);

CREATE TABLE maintenance_modes(
  id INT PRIMARY KEY,
  name VARCHAR NOT NULL,

  UNIQUE(name)
);

CREATE TABLE host_attributes(
  id INT IDENTITY,
  host VARCHAR NOT NULL,
  mode TINYINT NOT NULL REFERENCES maintenance_modes(id),
  slave_id VARCHAR NOT NULL,

  UNIQUE(host),
  UNIQUE(slave_id),
);

CREATE TABLE host_attribute_values(
  id INT IDENTITY,
  host_attribute_id INT NOT NULL REFERENCES host_attributes(id)
  ON DELETE CASCADE,
  name VARCHAR NOT NULL,
  value VARCHAR NOT NULL,

  UNIQUE(host_attribute_id, name, value)
);

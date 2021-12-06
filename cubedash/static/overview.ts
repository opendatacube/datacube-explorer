/// <reference path="../../node_modules/@types/leaflet/index.d.ts"/>
/// <reference path="../../node_modules/@types/geojson/index.d.ts"/>

class DataLayer {
    constructor(public name: string,
                public dataURL: string,
                public layer: L.GeoJSON,
                public data: GeoJSON.Feature | null = null,
                public showAlongside: DataLayer[] = []) {
    }
}

class ApplicationRoutes {
    constructor(
        public regionSearchURLPattern: string,
        public regionViewURLPattern: string,
        public datasetURLPattern: string,
        public geojsonRegionsURL: string,
        public geojsonDatasetsURL: string,
        public geojsonFootprintURL: string,
    ) {
    }

    public getRegionSearchURL(regionCode:string) {
        return this.regionSearchURLPattern.replace('__REGION_CODE__', regionCode);
    }

    public getRegionViewURL(regionCode:string) {
        return this.regionViewURLPattern.replace('__REGION_CODE__', regionCode);
    }

    public getDatasetViewURL(datasetId:string) {
        return this.datasetURLPattern.replace('__DATASET_ID__', datasetId);
    }
}

class RecenterMapControl extends L.Control {
    _div: HTMLElement = L.DomUtil.create('div', 'recenter-map');
    _map: L.Map | null = null;
    _isDirty = false;
    _button: HTMLElement = L.DomUtil.create('button', 'small');

    constructor(public targetLayer: L.FeatureGroup) {
        super({position: "bottomleft"});
        this._button.innerText = 'Recenter';
    }

    public onAdd(map: L.Map): HTMLElement {
        this._map = map;
        this._map.on("moveend", () => {
            if (!this._isDirty && this._div) {
                this._isDirty = true;
                this._div.appendChild(this._button);
            }
        });
        this._button.addEventListener('click', () => {
            this.doRecenter();
        });
        this._isDirty = false;
        return this._div;
    };

    public doRecenter() {
        if (this.targetLayer && this._map) {
            this._map.fitBounds(this.targetLayer.getBounds(), {
                animate: false,
                maxZoom: 6
            });
            this._div.removeChild(this._button);
            this._isDirty = false;
        }
    }
}

class FootprintLayer extends L.GeoJSON {
    constructor(footprintData: GeoJSON.Feature,
                showAlone = false) {
        super(footprintData, {
            interactive: false,
            style: function (feature) {
                return {
                    color: "#00A1DE",
                    fill: showAlone,
                    fillColor: "#8FCAE7",
                    opacity: 0.3,
                    weight: 2,
                    clickable: false
                };
            }
        });
    }
}


class RegionsLayer extends L.GeoJSON {
    constructor(regionData: GeoJSON.Feature,
                routes: ApplicationRoutes) {

        function getBin(v: number,
                        bin_count: number,
                        min_v: number,
                        max_v: number): number {
            let range = max_v - min_v,
                val = v - min_v;

            if (range < bin_count) {
                const padding = bin_count - range;
                return padding + val - 1;
            } else {
                const bin_width = range / bin_count;
                return Math.floor(val / bin_width);
            }
        }

        function getColor(count: number,
                          min_count: number,
                          max_count: number): string {
            let colorSteps = ['#eff3ff', '#c6dbef', '#9ecae1', '#6baed6', '#3182bd', '#08519c'],
                bin = getBin(count, colorSteps.length - 1, min_count, max_count);
            return colorSteps[bin];
        }
        super(regionData, {
            style: (feature): L.PathOptions => {
                if (!regionData.properties) {
                    throw Error("Invalid data: no properties")
                }
                const min_v = regionData.properties.min_count,
                    max_v = regionData.properties.max_count,
                    count = feature?.properties?.count,
                    color = getColor(count, min_v, max_v);
                return {
                    color: "#f2f2f2",
                    fill: true,
                    fillColor: color,
                    opacity: 0.6,
                    fillOpacity: 0.4,
                    weight: 1,
                };
            },
            onEachFeature: (feature, layer) => {
                let props = feature.properties,
                    template = `<div>
                                    <strong>${props.label || props.region_code}</strong>
                                </div>
                                ${props.count} dataset${props.count === 1 ? '' : 's'}`;
                layer.bindTooltip(template, {
                    className: 'regions-tooltip',
                    opacity: 1,
                })
                layer.on({
                    mouseover: function (e) {
                        const layer = e.target;

                        layer.setStyle({
                            color: '#375400',
                        });
                    }
                    ,
                    mouseout: (e) => {
                        this.resetStyle(e.target);
                    },
                    click: (e) => {
                        let props = e.target.feature.properties;
                        // If only one, jump straight to that dataset.
                        if (props.count === 1) {
                            window.location.href = routes.getRegionViewURL(props.region_code);
                        } else {
                            window.location.href = routes.getRegionSearchURL(props.region_code)
                        }
                    }
                });
            }
        });
    }
}


class DatasetsLayer extends L.GeoJSON {
    constructor(routes: ApplicationRoutes) {
        super(undefined, {
            style: function (feature) {
                return {
                    color: "#637c6b",
                    fill: true,
                    fillColor: "#082e41",
                    opacity: 0.3,
                    weight: 2,
                    clickable: true
                };
            },
            onEachFeature: (feature, layer) => {
                const props = feature.properties,
                      template = `<div>
                                    <strong>
                                        ${props.label || props['cubedash:region_code'] || ''}
                                    </strong>
                                    <div>${props['datetime']}</div>
                                  </div>`;
                layer.bindTooltip(template, {
                    className: 'datasets-tooltip',
                    opacity: 1,
                })

                layer.on({
                    mouseover: function (e) {
                        const layer = e.target;
                        layer.setStyle({
                            color: '#375400',
                            fillOpacity: 0.6,
                        });
                    },
                    mouseout: (e) => {
                        this.resetStyle(e.target);
                    },
                    click: function (e) {
                        let feature = e.target.feature;
                        window.location.href = routes.getDatasetViewURL(feature.id);
                    }
                });
            }
        });
    }
}

class OverviewMap extends L.Map {
    constructor(private dataLayers: DataLayer[],
                activeLayer: DataLayer | null,
                defaultZoom: number,
                defaultCenter: L.LatLngTuple) {
        super("map", {
            zoom: defaultZoom,
            center: defaultCenter,
            layers: [
                L.tileLayer(
                    "//cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png",
                    {
                        maxZoom: 19,
                        attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors,' +
                            ' &copy; <a href="https://cartodb.com/attributions">CartoDB</a>'
                    }
                )
            ],
            zoomControl: false,
            attributionControl: false,
            scrollWheelZoom: false
        });
        L.control.zoom({position: "bottomright"}).addTo(this);

        if (activeLayer) {
            const recenter = new RecenterMapControl(activeLayer.layer);

            for (const dataLayer of dataLayers) {
                const optBox: HTMLOptionElement = getViewToggle(dataLayer.name);
                optBox.selected = true;
                if (dataLayer.data) {
                    optBox.disabled = false;
                } else {
                    requestData(
                        dataLayer.name,
                        dataLayer.dataURL,
                        (enabled) => (optBox.disabled = !enabled),
                        dataLayer.layer
                    );
                }
                optBox.addEventListener('click', () => {
                    this.changeActive(dataLayer);
                    recenter.targetLayer = dataLayer.layer;
                });
            }
            this.changeActive(activeLayer);

            recenter.addTo(this);
            recenter.doRecenter();
        }

    };

    public changeActive(d: DataLayer) {
        for (const otherD of this.dataLayers)
            if (otherD !== d)
                this.removeLayer(otherD.layer);
        this.addLayer(d.layer);
        for (const pairedD of d.showAlongside)
            this.addLayer(pairedD.layer);
    };
}

function initPage(hasDisplayableData: boolean,
                  showIndividualDatasets: boolean,
                  routes: ApplicationRoutes,
                  regionData: GeoJSON.Feature,
                  footprintData: GeoJSON.Feature,
                  defaultZoom:number,
                  defaultCenter: L.LatLngTuple) {

    const layers = [];
    let activeLayer = null;

    if (hasDisplayableData) {
        const footprint = new DataLayer(
            'footprint',
            routes.geojsonFootprintURL,
            new FootprintLayer(footprintData, !regionData),
            footprintData
        );

        if (regionData) {
            layers.push(
                new DataLayer('regions', routes.geojsonRegionsURL,
                    new RegionsLayer(
                        regionData,
                        routes,
                    ),
                    regionData,
                    [footprint])
            )
        } else {
            layers.push(footprint);
        }
        activeLayer = layers[0];

        if (showIndividualDatasets) {
            layers.push(new DataLayer(
                'datasets',
                routes.geojsonDatasetsURL,
                new DatasetsLayer(routes)
            ));
        }
    }
    return new OverviewMap(layers, activeLayer, defaultZoom, defaultCenter);
}

function getViewToggle(name: string): HTMLOptionElement {
    const el = document.querySelector('input[name="map_display_view"][value="' + name + '"]')
    if (!el) {
        throw new Error(`No option box on page for ${name}`)
    }
    return <HTMLOptionElement>el
}


function requestData(name: string,
                     url: string,
                     setEnabled: (enabled: boolean) => void,
                     dataLayer: L.GeoJSON) {

    function showError(msg: string) {
        // TODO: message box?
        let er = document.getElementById('quiet-page-errors');
        if (er) {
            er.innerHTML += msg + '<br/>';
        }
    }

    const request = new XMLHttpRequest();

    setEnabled(false);
    request.open('GET', url, true);
    request.onload = function () {
        if (request.status >= 200 && request.status < 400) {
            const geojsonResponse = JSON.parse(request.responseText);
            if (geojsonResponse && geojsonResponse.features && geojsonResponse.features.length > 0) {
                dataLayer.addData(geojsonResponse);
                setEnabled(true);
            }
        } else {
            // We reached our target server, but it returned an error
            showError(`Error fetching ${name}`);
        }
    };
    request.onerror = function () {
        // There was a connection error of some sort
        showError(`Error fetching ${name}`)
    };
    request.send();
}

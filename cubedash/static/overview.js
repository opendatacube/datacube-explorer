"use strict";
/// <reference path="../../node_modules/@types/leaflet/index.d.ts"/>
/// <reference path="../../node_modules/@types/geojson/index.d.ts"/>
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var DataLayer = /** @class */ (function () {
    function DataLayer(name, dataURL, layer, data, showAlongside) {
        if (data === void 0) { data = null; }
        if (showAlongside === void 0) { showAlongside = []; }
        this.name = name;
        this.dataURL = dataURL;
        this.layer = layer;
        this.data = data;
        this.showAlongside = showAlongside;
    }
    return DataLayer;
}());
var ApplicationRoutes = /** @class */ (function () {
    function ApplicationRoutes(regionSearchURLPattern, regionViewURLPattern, datasetURLPattern, geojsonRegionsURL, geojsonDatasetsURL, geojsonFootprintURL) {
        this.regionSearchURLPattern = regionSearchURLPattern;
        this.regionViewURLPattern = regionViewURLPattern;
        this.datasetURLPattern = datasetURLPattern;
        this.geojsonRegionsURL = geojsonRegionsURL;
        this.geojsonDatasetsURL = geojsonDatasetsURL;
        this.geojsonFootprintURL = geojsonFootprintURL;
    }
    ApplicationRoutes.prototype.getRegionSearchURL = function (regionCode) {
        return this.regionSearchURLPattern.replace('__REGION_CODE__', regionCode);
    };
    ApplicationRoutes.prototype.getRegionViewURL = function (regionCode) {
        return this.regionViewURLPattern.replace('__REGION_CODE__', regionCode);
    };
    ApplicationRoutes.prototype.getDatasetViewURL = function (datasetId) {
        return this.datasetURLPattern.replace('__DATASET_ID__', datasetId);
    };
    return ApplicationRoutes;
}());
var RecenterMapControl = /** @class */ (function (_super) {
    __extends(RecenterMapControl, _super);
    function RecenterMapControl(targetLayer) {
        var _this = _super.call(this, { position: "bottomleft" }) || this;
        _this.targetLayer = targetLayer;
        _this._div = L.DomUtil.create('div', 'recenter-map');
        _this._map = null;
        _this._isDirty = false;
        _this._button = L.DomUtil.create('button', 'small');
        _this._button.innerText = 'Recenter';
        return _this;
    }
    RecenterMapControl.prototype.onAdd = function (map) {
        var _this = this;
        this._map = map;
        this._map.on("moveend", function () {
            if (!_this._isDirty && _this._div) {
                _this._isDirty = true;
                _this._div.appendChild(_this._button);
            }
        });
        this._button.addEventListener('click', function () {
            _this.doRecenter();
        });
        this._isDirty = false;
        return this._div;
    };
    ;
    RecenterMapControl.prototype.doRecenter = function () {
        if (this.targetLayer && this._map) {
            this._map.fitBounds(this.targetLayer.getBounds(), {
                animate: false,
                maxZoom: 6
            });
            this._div.removeChild(this._button);
            this._isDirty = false;
        }
    };
    return RecenterMapControl;
}(L.Control));
var FootprintLayer = /** @class */ (function (_super) {
    __extends(FootprintLayer, _super);
    function FootprintLayer(footprintData, showAlone) {
        if (showAlone === void 0) { showAlone = false; }
        return _super.call(this, footprintData, {
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
        }) || this;
    }
    return FootprintLayer;
}(L.GeoJSON));
var RegionsLayer = /** @class */ (function (_super) {
    __extends(RegionsLayer, _super);
    function RegionsLayer(regionData, routes) {
        var _this = this;
        function getBin(v, bin_count, min_v, max_v) {
            var range = max_v - min_v, val = v - min_v;
            if (range < bin_count) {
                var padding = bin_count - range;
                return padding + val - 1;
            }
            else {
                var bin_width = range / bin_count;
                return Math.floor(val / bin_width);
            }
        }
        function getColor(count, min_count, max_count) {
            var colorSteps = ['#eff3ff', '#c6dbef', '#9ecae1', '#6baed6', '#3182bd', '#08519c'], bin = getBin(count, colorSteps.length - 1, min_count, max_count);
            return colorSteps[bin];
        }
        _this = _super.call(this, regionData, {
            style: function (feature) {
                var _a;
                if (!regionData.properties) {
                    throw Error("Invalid data: no properties");
                }
                var min_v = regionData.properties.min_count, max_v = regionData.properties.max_count, count = (_a = feature === null || feature === void 0 ? void 0 : feature.properties) === null || _a === void 0 ? void 0 : _a.count, color = getColor(count, min_v, max_v);
                return {
                    color: "#f2f2f2",
                    fill: true,
                    fillColor: color,
                    opacity: 0.6,
                    fillOpacity: 0.4,
                    weight: 1,
                };
            },
            onEachFeature: function (feature, layer) {
                var props = feature.properties, template = "<div>\n                                    <strong>" + (props.label || props.region_code) + "</strong>\n                                </div>\n                                " + props.count + " dataset" + (props.count === 1 ? '' : 's');
                layer.bindTooltip(template, {
                    className: 'regions-tooltip',
                    opacity: 1,
                });
                layer.on({
                    mouseover: function (e) {
                        var layer = e.target;
                        layer.setStyle({
                            color: '#375400',
                        });
                    },
                    mouseout: function (e) {
                        _this.resetStyle(e.target);
                    },
                    click: function (e) {
                        var props = e.target.feature.properties;
                        // If only one, jump straight to that dataset.
                        if (props.count === 1) {
                            window.location.href = routes.getRegionViewURL(props.region_code);
                        }
                        else {
                            window.location.href = routes.getRegionSearchURL(props.region_code);
                        }
                    }
                });
            }
        }) || this;
        return _this;
    }
    return RegionsLayer;
}(L.GeoJSON));
var DatasetsLayer = /** @class */ (function (_super) {
    __extends(DatasetsLayer, _super);
    function DatasetsLayer(routes) {
        var _this = _super.call(this, undefined, {
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
            onEachFeature: function (feature, layer) {
                var props = feature.properties, template = "<div>\n                                    <strong>\n                                        " + (props.label || props['cubedash:region_code'] || '') + "\n                                    </strong>\n                                    <div>" + props['datetime'] + "</div>\n                                  </div>";
                layer.bindTooltip(template, {
                    className: 'datasets-tooltip',
                    opacity: 1,
                });
                layer.on({
                    mouseover: function (e) {
                        var layer = e.target;
                        layer.setStyle({
                            color: '#375400',
                            fillOpacity: 0.6,
                        });
                    },
                    mouseout: function (e) {
                        _this.resetStyle(e.target);
                    },
                    click: function (e) {
                        var feature = e.target.feature;
                        window.location.href = routes.getDatasetViewURL(feature.id);
                    }
                });
            }
        }) || this;
        return _this;
    }
    return DatasetsLayer;
}(L.GeoJSON));
var OverviewMap = /** @class */ (function (_super) {
    __extends(OverviewMap, _super);
    function OverviewMap(dataLayers, activeLayer, defaultZoom, defaultCenter) {
        var _this = _super.call(this, "map", {
            zoom: defaultZoom,
            center: defaultCenter,
            layers: [
                L.tileLayer("//cartodb-basemaps-{s}.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png", {
                    maxZoom: 19,
                    attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors,' +
                        ' &copy; <a href="https://cartodb.com/attributions">CartoDB</a>'
                })
            ],
            zoomControl: false,
            attributionControl: false,
            scrollWheelZoom: false
        }) || this;
        _this.dataLayers = dataLayers;
        L.control.zoom({ position: "bottomright" }).addTo(_this);
        if (activeLayer) {
            var recenter_1 = new RecenterMapControl(activeLayer.layer);
            var _loop_1 = function (dataLayer) {
                var optBox = getViewToggle(dataLayer.name);
                optBox.selected = true;
                if (dataLayer.data) {
                    optBox.disabled = false;
                }
                else {
                    requestData(dataLayer.name, dataLayer.dataURL, function (enabled) { return (optBox.disabled = !enabled); }, dataLayer.layer);
                }
                optBox.addEventListener('click', function () {
                    _this.changeActive(dataLayer);
                    recenter_1.targetLayer = dataLayer.layer;
                });
            };
            for (var _i = 0, dataLayers_1 = dataLayers; _i < dataLayers_1.length; _i++) {
                var dataLayer = dataLayers_1[_i];
                _loop_1(dataLayer);
            }
            _this.changeActive(activeLayer);
            recenter_1.addTo(_this);
            recenter_1.doRecenter();
        }
        return _this;
    }
    ;
    OverviewMap.prototype.changeActive = function (d) {
        for (var _i = 0, _a = this.dataLayers; _i < _a.length; _i++) {
            var otherD = _a[_i];
            if (otherD !== d)
                this.removeLayer(otherD.layer);
        }
        this.addLayer(d.layer);
        for (var _b = 0, _c = d.showAlongside; _b < _c.length; _b++) {
            var pairedD = _c[_b];
            this.addLayer(pairedD.layer);
        }
    };
    ;
    return OverviewMap;
}(L.Map));
function initPage(hasDisplayableData, showIndividualDatasets, routes, regionData, footprintData, defaultZoom, defaultCenter) {
    var layers = [];
    var activeLayer = null;
    if (hasDisplayableData) {
        var footprint = new DataLayer('footprint', routes.geojsonFootprintURL, new FootprintLayer(footprintData, !regionData), footprintData);
        if (regionData) {
            layers.push(new DataLayer('regions', routes.geojsonRegionsURL, new RegionsLayer(regionData, routes), regionData, [footprint]));
        }
        else {
            layers.push(footprint);
        }
        activeLayer = layers[0];
        if (showIndividualDatasets) {
            layers.push(new DataLayer('datasets', routes.geojsonDatasetsURL, new DatasetsLayer(routes)));
        }
    }
    return new OverviewMap(layers, activeLayer, defaultZoom, defaultCenter);
}
function getViewToggle(name) {
    var el = document.querySelector('input[name="map_display_view"][value="' + name + '"]');
    if (!el) {
        throw new Error("No option box on page for " + name);
    }
    return el;
}
function requestData(name, url, setEnabled, dataLayer) {
    function showError(msg) {
        // TODO: message box?
        var er = document.getElementById('quiet-page-errors');
        if (er) {
            er.innerHTML += msg + '<br/>';
        }
    }
    var request = new XMLHttpRequest();
    setEnabled(false);
    request.open('GET', url, true);
    request.onload = function () {
        if (request.status >= 200 && request.status < 400) {
            var geojsonResponse = JSON.parse(request.responseText);
            if (geojsonResponse && geojsonResponse.features && geojsonResponse.features.length > 0) {
                dataLayer.addData(geojsonResponse);
                setEnabled(true);
            }
        }
        else {
            // We reached our target server, but it returned an error
            showError("Error fetching " + name);
        }
    };
    request.onerror = function () {
        // There was a connection error of some sort
        showError("Error fetching " + name);
    };
    request.send();
}
//# sourceMappingURL=overview.js.map

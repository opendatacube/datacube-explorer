"use strict";
/// <reference path="../../node_modules/@types/leaflet/index.d.ts"/>
/// <reference path="../../node_modules/@types/geojson/index.d.ts"/>
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
        return extendStatics(d, b);
    }
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var DataLayer = /** @class */ (function () {
    function DataLayer(name, data_url, layer, data, show_alongside) {
        if (data === void 0) { data = null; }
        if (show_alongside === void 0) { show_alongside = []; }
        this.name = name;
        this.data_url = data_url;
        this.layer = layer;
        this.data = data;
        this.show_alongside = show_alongside;
    }
    return DataLayer;
}());
var TimeSummaryRoutes = /** @class */ (function () {
    function TimeSummaryRoutes(region_search_pattern, region_view_pattern, geojson_regions_url, geojson_datasets_url, geojson_footprint_url) {
        this.region_search_pattern = region_search_pattern;
        this.region_view_pattern = region_view_pattern;
        this.geojson_regions_url = geojson_regions_url;
        this.geojson_datasets_url = geojson_datasets_url;
        this.geojson_footprint_url = geojson_footprint_url;
    }
    return TimeSummaryRoutes;
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
var DatasetInfoControl = /** @class */ (function (_super) {
    __extends(DatasetInfoControl, _super);
    function DatasetInfoControl() {
        var _this = _super.call(this, { position: "bottomleft" }) || this;
        _this._div = L.DomUtil.create('div', 'dataset-info');
        return _this;
    }
    DatasetInfoControl.prototype.onAdd = function (map) {
        this.update();
        return this._div;
    };
    ;
    DatasetInfoControl.prototype.update = function (template) {
        if (template) {
            this._div.innerHTML = template;
        }
        else {
            this._div.innerHTML = '';
        }
    };
    return DatasetInfoControl;
}(L.Control));
var FootprintLayer = /** @class */ (function (_super) {
    __extends(FootprintLayer, _super);
    function FootprintLayer(footprint_data, showAlone) {
        if (showAlone === void 0) { showAlone = false; }
        return _super.call(this, footprint_data, {
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
    function RegionsLayer(region_data, control, routes) {
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
        // @ts-ignore (https://github.com/DefinitelyTyped/DefinitelyTyped/issues/9257)
        _this = _super.call(this, region_data, {
            style: function (feature) {
                if (!region_data.properties) {
                    throw Error("Invalid data: no properties");
                }
                var min_v = region_data.properties.min_count, max_v = region_data.properties.max_count, count = feature.properties.count, color = getColor(count, min_v, max_v);
                return {
                    color: "#f2f2f2",
                    fill: true,
                    fillColor: color,
                    opacity: 0.6,
                    fillOpacity: 0.4,
                    weight: 1,
                    clickable: true
                };
            },
            onEachFeature: function (feature, layer) {
                layer.on({
                    mouseover: function (e) {
                        var layer = e.target;
                        layer.setStyle({
                            color: '#375400',
                        });
                        var props = layer.feature.properties, template = "<div>\n                                            <strong>" + (props.label || props.region_code) + "</strong>\n                                        </div>\n                                        " + props.count + " dataset" + (props.count === 1 ? '' : 's');
                        control.update(template);
                    },
                    mouseout: function (e) {
                        _this.resetStyle(e.target);
                        control.update();
                    },
                    click: function (e) {
                        var props = e.target.feature.properties, url_pattern = routes.region_search_pattern;
                        // If only one, jump straight to that dataset.
                        if (props.count === 1) {
                            url_pattern = routes.region_view_pattern;
                        }
                        window.location.href = url_pattern.replace('__REGION_CODE__', props.region_code);
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
    function DatasetsLayer(infoControl) {
        var _this = _super.call(this, undefined, {
            style: function (feature) {
                return {
                    color: "#7AB800",
                    fill: true,
                    fillColor: "#9aee00",
                    opacity: 0.3,
                    weight: 2,
                    clickable: true
                };
            },
            onEachFeature: function (feature, layer) {
                layer.on({
                    mouseover: function (e) {
                        var layer = e.target;
                        layer.setStyle({
                            color: '#375400',
                            fillOpacity: 0.6,
                        });
                        var props = layer.feature.properties, template = "<div><strong>" + props.label + "</strong></div>" + props.start_time;
                        infoControl.update(template);
                    },
                    mouseout: function (e) {
                        _this.resetStyle(e.target);
                        infoControl.update();
                    },
                    click: function (e) {
                        var props = e.target.feature.properties;
                        window.location.href = '/dataset/' + props.id;
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
    function OverviewMap(dataLayers, activeLayer) {
        var _this = _super.call(this, "map", {
            zoom: 3,
            center: [-26.2756326, 134.9387844],
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
                    requestData(dataLayer.name, dataLayer.data_url, function (enabled) { return (optBox.disabled = !enabled); }, dataLayer.layer);
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
            var d2 = _a[_i];
            if (d2 !== d)
                this.removeLayer(d2.layer);
        }
        this.addLayer(d.layer);
        for (var _b = 0, _c = d.show_alongside; _b < _c.length; _b++) {
            var paired = _c[_b];
            this.addLayer(paired.layer);
        }
    };
    ;
    return OverviewMap;
}(L.Map));
function initPage(has_displayable_data, show_individual_datasets, routes, region_data, footprint_data) {
    var layers = [];
    var activeLayer = null;
    var infoControl = new DatasetInfoControl();
    if (has_displayable_data) {
        var footprint = new DataLayer('footprint', routes.geojson_footprint_url, new FootprintLayer(footprint_data, !region_data), footprint_data);
        if (region_data) {
            layers.push(new DataLayer('regions', routes.geojson_regions_url, new RegionsLayer(region_data, infoControl, routes), region_data, [footprint]));
        }
        else {
            layers.push(footprint);
        }
        activeLayer = layers[0];
        if (show_individual_datasets) {
            layers.push(new DataLayer('datasets', routes.geojson_datasets_url, new DatasetsLayer(infoControl)));
        }
    }
    var map = new OverviewMap(layers, activeLayer);
    if (has_displayable_data) {
        infoControl.addTo(map);
    }
    return map;
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
        document.getElementById('quiet-page-errors').innerHTML += msg + '<br/>';
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
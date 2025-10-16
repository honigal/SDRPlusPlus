#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <RSA_API.h>

#include <utils/flog.h>
#include <module.h>
#include <gui/gui.h>
#include <gui/widgets/stepped_slider.h>
#include <gui/style.h>
#include <gui/smgui.h>
#include <signal_path/signal_path.h>
#include <core.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <limits>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <utility>

#define CONCAT(a, b) ((std::string(a) + b).c_str())

SDRPP_MOD_INFO{
    /* Name:            */ "rsa306_source",
    /* Description:     */ "Tektronix RSA306 source module",
    /* Author:          */ "Codex",
    /* Version:         */ 0, 2, 0,
    /* Max instances    */ 1
};

namespace {

constexpr double DEFAULT_CENTER_FREQ = 100e6;
constexpr double DEFAULT_REF_LEVEL = -30.0;
constexpr size_t DEFAULT_IQ_BUFFER_SAMPLES = 65536;

std::string hzToString(double value)
{
    std::ostringstream oss;
    if (value >= 1e9) {
        oss << std::fixed << std::setprecision(3) << (value / 1e9) << " GHz";
    }
    else if (value >= 1e6) {
        oss << std::fixed << std::setprecision(2) << (value / 1e6) << " MHz";
    }
    else if (value >= 1e3) {
        oss << std::fixed << std::setprecision(2) << (value / 1e3) << " kHz";
    }
    else {
        oss << std::fixed << std::setprecision(0) << value << " Hz";
    }
    return oss.str();
}

bool reportStatus(RSA_API::ReturnStatus status, const char* action)
{
    if (status == RSA_API::noError) {
        return true;
    }
    const char* err = RSA_API::DEVICE_GetErrorString(status);
    flog::error("RSA306: {} failed ({})", action, err ? err : "unknown error");
    return false;
}

struct DeviceInfo
{
    int id = -1;
    std::string serial;
    std::string type;
    std::string label;
};

struct BandwidthOption
{
    double bandwidth = 0.0;
    double sampleRate = 0.0;
    std::string label;
};

} // namespace

ConfigManager config;

class RSA306Module : public ModuleManager::Instance {
public:
    explicit RSA306Module(std::string instanceName)
        : name(std::move(instanceName))
    {
        refresh();

        config.acquire();
        std::string savedDevice = config.conf["device"];
        config.release();

        selectDevice(savedDevice);

        handler.ctx = this;
        handler.selectHandler = menuSelected;
        handler.deselectHandler = menuDeselected;
        handler.menuHandler = menuHandler;
        handler.startHandler = start;
        handler.stopHandler = stop;
        handler.tuneHandler = tune;
        handler.stream = &stream;

        sigpath::sourceManager.registerSource("Tektronix RSA306", &handler);
    }

    ~RSA306Module() override
    {
        stop(this);
        disconnectDevice();
        sigpath::sourceManager.unregisterSource("Tektronix RSA306");
    }

    void postInit() override {}

    void enable() override { enabled = true; }
    void disable() override { enabled = false; }
    bool isEnabled() override { return enabled; }

private:
    void refresh()
    {
        devices.clear();
        txtDeviceList.clear();

        int numDevices = 0;
        int ids[RSA_API::DEVSRCH_MAX_NUM_DEVICES] = {0};
        char serials[RSA_API::DEVSRCH_MAX_NUM_DEVICES][RSA_API::DEVSRCH_SERIAL_MAX_STRLEN] = {{0}};
        char types[RSA_API::DEVSRCH_MAX_NUM_DEVICES][RSA_API::DEVSRCH_TYPE_MAX_STRLEN] = {{0}};

        if (!reportStatus(RSA_API::DEVICE_Search(&numDevices, ids, serials, types), "DEVICE_Search")) {
            return;
        }

        for (int i = 0; i < numDevices; ++i) {
            DeviceInfo info;
            info.id = ids[i];
            info.serial = serials[i];
            info.type = types[i];
            info.label = "Tektronix RSA306 (" + info.serial + ")";

            devices.push_back(info);
            txtDeviceList += info.label;
            txtDeviceList += '\0';
        }

        if (devices.empty()) {
            txtDeviceList += "No devices";
            txtDeviceList += '\0';
            selectedDevice = -1;
        }
        else if (selectedDevice < 0) {
            selectedDevice = 0;
        }
    }

    void selectDevice(const std::string& label)
    {
        if (devices.empty()) {
            selectedDevice = -1;
            return;
        }

        int index = -1;
        for (size_t i = 0; i < devices.size(); ++i) {
            if (devices[i].label == label) {
                index = static_cast<int>(i);
                break;
            }
        }

        if (index < 0) {
            index = 0;
        }

        if (index == selectedDevice) {
            if (!connected) {
                connectDevice();
            }
            return;
        }

        if (running) {
            flog::warn("RSA306: stop the device before switching sources");
            return;
        }

        disconnectDevice();
        selectedDevice = index;
        connectDevice();
    }

    bool connectDevice()
    {
        if (connected || selectedDevice < 0 || selectedDevice >= static_cast<int>(devices.size())) {
            return connected;
        }

        const auto& dev = devices[selectedDevice];
        RSA_API::ReturnStatus connStatus = RSA_API::DEVICE_Connect(dev.id);
        if (connStatus != RSA_API::noError) {
            const char* connErr = RSA_API::DEVICE_GetErrorString(connStatus);
            flog::warn("RSA306: initial DEVICE_Connect failed ({}), attempting DEVICE_Reset",
                       connErr ? connErr : "unknown");
            RSA_API::DEVICE_Reset(dev.id);
            connStatus = RSA_API::DEVICE_Connect(dev.id);
            if (connStatus != RSA_API::noError) {
                reportStatus(connStatus, "DEVICE_Connect");
                return false;
            }
        }

        connected = true;

        if (!reportStatus(RSA_API::CONFIG_Preset(), "CONFIG_Preset")) {
            disconnectDevice();
            return false;
        }

        RSA_API::DEVICE_INFO info{};
        if (reportStatus(RSA_API::DEVICE_GetInfo(&info), "DEVICE_GetInfo")) {
            deviceInfo = info;
        }

        double minCF = 0.0;
        double maxCF = 0.0;
        if (reportStatus(RSA_API::CONFIG_GetMinCenterFreq(&minCF), "CONFIG_GetMinCenterFreq")) {
            minCenterFreq = minCF;
        }
        if (reportStatus(RSA_API::CONFIG_GetMaxCenterFreq(&maxCF), "CONFIG_GetMaxCenterFreq")) {
            maxCenterFreq = maxCF;
        }

        loadConfiguration();
        populateBandwidthOptions();
        applyStaticSettings();

        return true;
    }

    void disconnectDevice()
    {
        if (!connected || running) {
            return;
        }
        reportStatus(RSA_API::DEVICE_Disconnect(), "DEVICE_Disconnect");
        connected = false;
    }

    void populateBandwidthOptions()
    {
        bandwidthOptions.clear();
        txtBandwidthList.clear();

        if (!connected) {
            return;
        }

        double minBW = 0.0;
        double maxBW = 0.0;
        if (!reportStatus(RSA_API::IQSTREAM_GetMinAcqBandwidth(&minBW), "IQSTREAM_GetMinAcqBandwidth") ||
            !reportStatus(RSA_API::IQSTREAM_GetMaxAcqBandwidth(&maxBW), "IQSTREAM_GetMaxAcqBandwidth")) {
            return;
        }
        if (minBW <= 0.0 || maxBW <= 0.0) { return; }

        flog::info("RSA306: IQ bandwidth range {:.0f} Hz â€“ {:.0f} Hz", minBW, maxBW);

        std::vector<double> candidates = {
            minBW,
            std::sqrt(minBW * maxBW),
            maxBW / 4.0,
            maxBW / 2.0,
            maxBW
        };

        std::vector<double> seenSampleRates;
        for (double candidate : candidates) {
            double clamped = std::max(minBW, std::min(maxBW, candidate));
            RSA_API::ReturnStatus rs = RSA_API::IQSTREAM_SetAcqBandwidth(clamped);
            if (rs != RSA_API::noError) {
                flog::warn("RSA306: IQSTREAM_SetAcqBandwidth({}) failed: {}", hzToString(clamped), RSA_API::DEVICE_GetErrorString(rs));
                continue;
            }
            double bwAct = 0.0;
            double sr = 0.0;
            if (!reportStatus(RSA_API::IQSTREAM_GetAcqParameters(&bwAct, &sr), "IQSTREAM_GetAcqParameters")) {
                continue;
            }
            if (sr <= 0.0) { continue; }

            bool duplicate = std::any_of(seenSampleRates.begin(), seenSampleRates.end(),
                                         [sr](double existing) { return std::fabs(existing - sr) < 1.0; });
            if (duplicate) { continue; }

            BandwidthOption option;
            option.bandwidth = bwAct > 0.0 ? bwAct : clamped;
            option.sampleRate = sr;
            option.label = hzToString(option.bandwidth);
            bandwidthOptions.push_back(option);
            seenSampleRates.push_back(sr);
        }

        if (bandwidthOptions.empty()) {
            BandwidthOption fallback;
            fallback.bandwidth = minBW;
            fallback.sampleRate = minBW * 2.0;
            fallback.label = hzToString(fallback.sampleRate);
            bandwidthOptions.push_back(fallback);
        }

        std::sort(bandwidthOptions.begin(), bandwidthOptions.end(),
                  [](const BandwidthOption& a, const BandwidthOption& b) { return a.sampleRate < b.sampleRate; });

        txtBandwidthList.clear();
        for (auto& option : bandwidthOptions) {
            txtBandwidthList += option.label;
            txtBandwidthList += '\0';
        }

        selectBandwidthIndex(findNearestBandwidthIndex(bandwidth));
    }

    int findNearestBandwidthIndex(double targetBW) const
    {
        if (bandwidthOptions.empty()) {
            return 0;
        }
        int bestIndex = 0;
        double bestDiff = std::numeric_limits<double>::max();
        for (size_t i = 0; i < bandwidthOptions.size(); ++i) {
            double diff = std::fabs(bandwidthOptions[i].bandwidth - targetBW);
            if (diff < bestDiff) {
                bestDiff = diff;
                bestIndex = static_cast<int>(i);
            }
        }
        return bestIndex;
    }

    void selectBandwidthIndex(int index)
    {
        if (bandwidthOptions.empty()) {
            return;
        }
        if (index < 0 || index >= static_cast<int>(bandwidthOptions.size())) {
            index = 0;
        }

        selectedBandwidth = index;
        bandwidth = bandwidthOptions[index].bandwidth;
        sampleRate = bandwidthOptions[index].sampleRate;
        core::setInputSampleRate(sampleRate);

        if (connected) {
            if (reportStatus(RSA_API::IQSTREAM_SetAcqBandwidth(bandwidth), "IQSTREAM_SetAcqBandwidth")) {
                double actualBw = 0.0;
                double actualSr = 0.0;
                if (reportStatus(RSA_API::IQSTREAM_GetAcqParameters(&actualBw, &actualSr), "IQSTREAM_GetAcqParameters")) {
                    if (actualBw > 0.0) {
                        bandwidth = actualBw;
                    }
                    if (actualSr > 0.0) {
                        sampleRate = actualSr;
                        core::setInputSampleRate(sampleRate);
                    }
                    flog::info("RSA306: applied bandwidth {} (sample rate {})",
                               hzToString(bandwidth),
                               hzToString(sampleRate));
                }
            }
        }
    }

    bool setHardwareCenterFrequency()
    {
        if (!connected) {
            return false;
        }
        if (!reportStatus(RSA_API::CONFIG_SetCenterFreq(centerFrequency), "CONFIG_SetCenterFreq")) {
            return false;
        }
        lastTunedFrequency = centerFrequency;
        return true;
    }

    void applyNonFrequencySettings()
    {
        if (!connected) {
            return;
        }

        reportStatus(RSA_API::CONFIG_SetReferenceLevel(referenceLevel), "CONFIG_SetReferenceLevel");
        reportStatus(RSA_API::CONFIG_SetRFPreampEnable(preampEnabled), "CONFIG_SetRFPreampEnable");
        reportStatus(RSA_API::CONFIG_SetAutoAttenuationEnable(autoAttenuation), "CONFIG_SetAutoAttenuationEnable");
    }

    void applyStaticSettings()
    {
        applyNonFrequencySettings();
        selectBandwidthIndex(selectedBandwidth);
        setHardwareCenterFrequency();
    }

    void loadConfiguration()
    {
        config.acquire();
        json stored = json::object();
        std::string key = devices[selectedDevice].label;
        if (config.conf["devices"].contains(key)) {
            stored = config.conf["devices"][key];
        }
        config.release();

        if (!stored.is_null() && !stored.empty()) {
            centerFrequency = stored.value("centerFrequency", DEFAULT_CENTER_FREQ);
            bandwidth = stored.value("bandwidth", 10e6);
            sampleRate = stored.value("sampleRate", 12.5e6);
            referenceLevel = stored.value("referenceLevel", DEFAULT_REF_LEVEL);
            preampEnabled = stored.value("preampEnabled", true);
            autoAttenuation = stored.value("autoAttenuation", true);
            selectedBandwidth = stored.value("bandwidthIndex", 0);
        }
        else {
            centerFrequency = DEFAULT_CENTER_FREQ;
            bandwidth = 10e6;
            sampleRate = 12.5e6;
            referenceLevel = DEFAULT_REF_LEVEL;
            preampEnabled = true;
            autoAttenuation = true;
            selectedBandwidth = 0;
        }
    }

    void saveConfiguration()
    {
        if (selectedDevice < 0 || selectedDevice >= static_cast<int>(devices.size())) {
            return;
        }
        std::string key = devices[selectedDevice].label;

        json entry;
        entry["centerFrequency"] = centerFrequency;
        entry["bandwidth"] = bandwidth;
        entry["sampleRate"] = sampleRate;
        entry["referenceLevel"] = referenceLevel;
        entry["preampEnabled"] = preampEnabled;
        entry["autoAttenuation"] = autoAttenuation;
        entry["bandwidthIndex"] = selectedBandwidth;

        config.acquire();
        config.conf["devices"][key] = entry;
        config.conf["device"] = key;
        config.release(true);
    }

    static void menuSelected(void* ctx)
    {
        auto* self = static_cast<RSA306Module*>(ctx);
        if (self->bandwidthOptions.empty()) {
            self->populateBandwidthOptions();
        }
        core::setInputSampleRate(self->sampleRate);
        flog::info("RSA306: '{}' menu selected", self->name);
    }

    static void menuDeselected(void* ctx)
    {
        auto* self = static_cast<RSA306Module*>(ctx);
        flog::info("RSA306: '{}' menu deselected", self->name);
    }

    static void start(void* ctx)
    {
        auto* self = static_cast<RSA306Module*>(ctx);
        if (self->running) {
            return;
        }
        if (self->selectedDevice < 0) {
            flog::error("RSA306: No device selected");
            return;
        }
        if (!self->connectDevice()) {
            flog::error("RSA306: Unable to connect to device");
            return;
        }

        self->applyStaticSettings();

        if (!reportStatus(RSA_API::DEVICE_Run(), "DEVICE_Run")) {
            return;
        }

        RSA_API::IQSTREAM_ClearAcqStatus();
        if (!reportStatus(RSA_API::IQSTREAM_SetOutputConfiguration(RSA_API::IQSOD_CLIENT, RSA_API::IQSODT_SINGLE),
                          "IQSTREAM_SetOutputConfiguration")) {
            return;
        }

        int maxBuffer = 0;
        if (reportStatus(RSA_API::IQSTREAM_GetIQDataBufferSize(&maxBuffer), "IQSTREAM_GetIQDataBufferSize")) {
            int desired = static_cast<int>(self->iqBufferSamples);
            desired = std::min(desired, maxBuffer);
            desired = std::max(desired, 1024);
            self->iqBufferSamples = desired;
        }

        if (!reportStatus(RSA_API::IQSTREAM_SetIQDataBufferSize(static_cast<int>(self->iqBufferSamples)),
                          "IQSTREAM_SetIQDataBufferSize")) {
            return;
        }

        RSA_API::IQSTREAM_ClearAcqStatus();
        if (!reportStatus(RSA_API::IQSTREAM_Start(), "IQSTREAM_Start")) {
            return;
        }

        self->iqBuffer.resize(self->iqBufferSamples * 2);
        self->running = true;
        self->stream.clearWriteStop();
        self->worker = std::thread(workerThread, self);
        }

    static void stop(void* ctx)
    {
        auto* self = static_cast<RSA306Module*>(ctx);
        if (!self->running) {
            return;
        }

        self->running = false;
        if (self->worker.joinable()) {
            self->worker.join();
        }
        reportStatus(RSA_API::IQSTREAM_Stop(), "IQSTREAM_Stop");
        reportStatus(RSA_API::DEVICE_Stop(), "DEVICE_Stop");

        }

    static void tune(double freq, void* ctx)
    {
        auto* self = static_cast<RSA306Module*>(ctx);
        double minCF = (self->minCenterFreq > 0.0 && self->maxCenterFreq > self->minCenterFreq)
                           ? self->minCenterFreq
                           : 0.0;
        double maxCF = (self->maxCenterFreq > self->minCenterFreq) ? self->maxCenterFreq : freq;
        if (minCF > 0.0 && maxCF > minCF) {
            self->centerFrequency = std::clamp(freq, minCF, maxCF);
        }
        else {
            self->centerFrequency = freq;
        }
        if (!self->connected) {
            return;
        }

        bool restart = self->running;
        if (restart) { stop(self); }

        double desiredFreq = self->centerFrequency;
        auto attempt = [&]() { return self->setHardwareCenterFrequency(); };

        if (!attempt()) {
            flog::warn("RSA306: attempting preset after failed center frequency change");
            if (reportStatus(RSA_API::CONFIG_Preset(), "CONFIG_Preset")) {
                self->applyNonFrequencySettings();
                self->selectBandwidthIndex(self->selectedBandwidth);
                self->centerFrequency = desiredFreq;
                if (!attempt()) {
                    flog::error("RSA306: center frequency change still failing after preset");
                    self->centerFrequency = self->lastTunedFrequency;
                    attempt();
                }
            }
            else {
                self->centerFrequency = self->lastTunedFrequency;
                attempt();
            }
        }

        if (restart) { start(self); }
    }

    static void workerThread(RSA306Module* self)
    {
        std::array<int, RSA_API::IQSTRM_MAXTRIGGERS> triggerIndices{};

        int debugIterations = 0;
        while (self->running) {
            bool ready = false;
            RSA_API::ReturnStatus waitStatus = RSA_API::IQSTREAM_WaitForIQDataReady(100, &ready);
            if (waitStatus != RSA_API::noError) {
                reportStatus(waitStatus, "IQSTREAM_WaitForIQDataReady");
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            if (!ready) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }

            int samplesAvailable = static_cast<int>(self->iqBufferSamples);
            RSA_API::IQSTRMIQINFO iqInfo{};
            iqInfo.triggerIndices = triggerIndices.data();

            RSA_API::ReturnStatus dataStatus =
                RSA_API::IQSTREAM_GetIQData(self->iqBuffer.data(), &samplesAvailable, &iqInfo);
            if (dataStatus != RSA_API::noError) {
                reportStatus(dataStatus, "IQSTREAM_GetIQData");
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            if (samplesAvailable <= 0) {
                flog::debug("RSA306: Wait succeeded but GetIQData returned no samples");
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }

            

            if (iqInfo.acqStatus & RSA_API::IQSTRM_STATUS_IBUFFOVFLOW) {
                flog::warn("RSA306: Input buffer overflow");
            }
            if (iqInfo.acqStatus & RSA_API::IQSTRM_STATUS_OBUFFOVFLOW) {
                flog::warn("RSA306: Output buffer overflow");
            }

            dsp::complex_t* dst = self->stream.writeBuf;
            const float* src = self->iqBuffer.data();
            const float scale = (iqInfo.scaleFactor != 0.0) ? static_cast<float>(iqInfo.scaleFactor) : 1.0f;

            for (int i = 0; i < samplesAvailable; ++i) {
                dst[i].re = src[2 * i] * scale;
                dst[i].im = src[2 * i + 1] * scale;
            }

            if (!self->stream.swap(samplesAvailable)) {
                flog::debug("RSA306: stream swap aborted (writer stop)");
                break;
            }
        }

        }

    static void menuHandler(void* ctx)
    {
        auto* self = static_cast<RSA306Module*>(ctx);

        if (self->devices.empty()) {
            SmGui::FillWidth();
            if (SmGui::Button(CONCAT("Refresh##_rsa306_refresh_", self->name))) {
                self->refresh();
                if (self->selectedDevice >= 0) {
                    self->selectDevice(self->devices[self->selectedDevice].label);
                }
            }
            SmGui::Text("No RSA306 devices detected.");
            return;
        }

        if (!self->txtDeviceList.empty()) {
            SmGui::LeftLabel("Device");
            SmGui::FillWidth();
            if (SmGui::Combo(CONCAT("##_rsa306_device_", self->name), &self->selectedDevice,
                             self->txtDeviceList.c_str())) {
                self->selectDevice(self->devices[self->selectedDevice].label);
                self->saveConfiguration();
            }
        }

        if (self->bandwidthOptions.empty()) {
            self->populateBandwidthOptions();
        }
        if (!self->txtBandwidthList.empty()) {
            SmGui::LeftLabel("Bandwidth");
            SmGui::FillWidth();
            if (SmGui::Combo(CONCAT("##_rsa306_bandwidth_", self->name), &self->selectedBandwidth,
                             self->txtBandwidthList.c_str())) {
                bool restart = self->running;
                if (restart) { stop(self); }

                self->selectBandwidthIndex(self->selectedBandwidth);
                self->saveConfiguration();

                if (restart) { start(self); }
            }
        }


        SmGui::LeftLabel("Reference Level (dBm)");
        SmGui::FillWidth();
        float refLevelUi = static_cast<float>(self->referenceLevel);
        if (SmGui::SliderFloat(CONCAT("##_rsa306_reflevel_", self->name), &refLevelUi, -130.0f, 20.0f)) {
            self->referenceLevel = static_cast<double>(refLevelUi);
            if (self->connected) {
                reportStatus(RSA_API::CONFIG_SetReferenceLevel(self->referenceLevel), "CONFIG_SetReferenceLevel");
            }
            self->saveConfiguration();
        }

    }

    std::string name;
    bool enabled = true;

    dsp::stream<dsp::complex_t> stream;
    SourceManager::SourceHandler handler{};

    std::atomic<bool> running{false};
    bool connected = false;
    std::thread worker;

    std::vector<DeviceInfo> devices;
    std::string txtDeviceList;
    int selectedDevice = -1;
    RSA_API::DEVICE_INFO deviceInfo{};

    std::vector<BandwidthOption> bandwidthOptions;
    std::string txtBandwidthList;
    int selectedBandwidth = 0;

    double centerFrequency = DEFAULT_CENTER_FREQ;
    double minCenterFreq = 0.0;
    double maxCenterFreq = 0.0;
    double bandwidth = 10e6;
    double sampleRate = 12.5e6;
    double referenceLevel = DEFAULT_REF_LEVEL;
    bool preampEnabled = true;
    bool autoAttenuation = true;
    double lastTunedFrequency = DEFAULT_CENTER_FREQ;

    size_t iqBufferSamples = DEFAULT_IQ_BUFFER_SAMPLES;
    std::vector<float> iqBuffer;
};

MOD_EXPORT void _INIT_()
{
    config.setPath(core::args["root"].s() + "/rsa306_source_config.json");
    json defaults;
    defaults["device"] = "";
    defaults["devices"] = json::object();
    config.load(defaults);
    config.enableAutoSave();
}

MOD_EXPORT ModuleManager::Instance* _CREATE_INSTANCE_(std::string name)
{
    return new RSA306Module(std::move(name));
}

MOD_EXPORT void _DELETE_INSTANCE_(ModuleManager::Instance* instance)
{
    delete static_cast<RSA306Module*>(instance);
}

MOD_EXPORT void _END_()
{
    config.disableAutoSave();
    config.save();
}















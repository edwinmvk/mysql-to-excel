"use client";

import React, { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Loader2, Upload, Database, FileSpreadsheet } from "lucide-react";

export default function SQLConverter() {
  const [formData, setFormData] = useState({
    username: "",
    password: "",
    host: "127.0.0.1",
  });
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState(false);

  const handleInputChange = (e) => {
    const { name, value } = e.target;
    setFormData((prev) => ({
      ...prev,
      [name]: value,
    }));
    setError("");
  };

  const handleFileChange = (e) => {
    if (e.target.files[0]) {
      setFile(e.target.files[0]);
      setError("");
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError("");
    setSuccess(false);

    if (!formData.username || !formData.password) {
      setError("Please provide both username and password");
      return;
    }

    if (!file) {
      setError("Please select a SQL file");
      return;
    }

    setLoading(true);

    try {
      const data = new FormData();
      data.append("username", formData.username);
      data.append("password", formData.password);
      data.append("host", formData.host);
      data.append("sqlFile", file);

      const response = await fetch("http://localhost:5000/convert", {
        method: "POST",
        body: data,
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || "Failed to convert file");
      }

      // blob() takes the server response (like a file) and converts it into a binary large object so the browser can handle it like a file
      const blob = await response.blob();
      // createObjectURL() creates a temporary URL for the blob, allowing the browser to access the data directly
      const downloadUrl = window.URL.createObjectURL(blob);
      // <a> tag element is created and its href is set to downloadUrl
      const link = document.createElement("a");
      link.href = downloadUrl;

      // Get filename from Content-Disposition header if available
      const contentDisposition = response.headers.get("Content-Disposition");
      // It looks for filename="..." and captures the filename within the quotes
      const filenameMatch =
        contentDisposition && contentDisposition.match(/filename="?([^"]+)"?/);
      const filename = filenameMatch
        ? filenameMatch[1]
        : "database_export.xlsx";

      // Trigger download and later clean up link to free the memory
      link.setAttribute("download", filename);
      document.body.appendChild(link);
      link.click();
      link.parentNode.removeChild(link);
      window.URL.revokeObjectURL(downloadUrl);

      setSuccess(true);
      // Reset file input
      setFile(null);
      // Reset form
      e.target.reset();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Card className="max-w-md mx-auto">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Database className="h-5 w-5" />
          Convert SQL to Excel
        </CardTitle>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-2">
            <Input
              type="text"
              name="username"
              placeholder="MySQL Username"
              onChange={handleInputChange}
              className="w-full"
            />
          </div>

          <div className="space-y-2">
            <Input
              type="password"
              name="password"
              placeholder="MySQL Password"
              onChange={handleInputChange}
              className="w-full"
            />
          </div>

          <div className="space-y-2">
            <Input
              type="text"
              name="host"
              placeholder="Host (default: 127.0.0.1)"
              onChange={handleInputChange}
              value={formData.host}
              className="w-full"
            />
          </div>

          <div className="space-y-2">
            <div className="flex items-center justify-center w-full">
              <label className="flex flex-col items-center justify-center w-full h-32 border-2 border-gray-300 border-dashed rounded-lg cursor-pointer bg-gray-50 hover:bg-gray-100">
                <div className="flex flex-col items-center justify-center pt-5 pb-6">
                  <Upload className="w-8 h-8 mb-3 text-gray-400" />
                  <p className="mb-2 text-sm text-gray-500">
                    <span className="font-semibold">Click to upload</span> or
                    drag and drop
                  </p>
                  <p className="text-xs text-gray-500">.sql file</p>
                </div>
                <Input
                  type="file"
                  accept=".sql"
                  onChange={handleFileChange}
                  className="hidden"
                />
              </label>
            </div>
            {file && (
              <p className="text-sm text-gray-500 mt-2">
                Selected file: {file.name}
              </p>
            )}
          </div>

          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          {success && (
            <Alert className="bg-green-50 text-green-700 border-green-200">
              <AlertDescription className="flex items-center gap-2">
                <FileSpreadsheet className="h-4 w-4" />
                Excel file downloaded successfully!
              </AlertDescription>
            </Alert>
          )}

          <Button type="submit" className="w-full" disabled={loading}>
            {loading ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Converting...
              </>
            ) : (
              <>
                <FileSpreadsheet className="mr-2 h-4 w-4" />
                Convert to Excel
              </>
            )}
          </Button>
        </form>
      </CardContent>
    </Card>
  );
}
